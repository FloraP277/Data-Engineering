"""Spark Structured Streaming job: request logs -> per-service windows -> alerts.

Source: Kafka (real time) or a folder of JSON files (local runs and tests).
For each 1-minute window per service it computes request count, error rate,
average and 95th-percentile latency. Late events are accepted for 2 minutes
(watermark) and duplicate deliveries are dropped by event_id.

Each micro-batch is upserted into the sinks by (service, window_start), so
re-processing a window never double-counts, then alert rules run on it.

    python -m apimon.streaming --source files --path data/events --out data/out --once
    python -m apimon.streaming --source kafka --bootstrap localhost:9092 --topic api-requests \
        --pg postgresql://postgres:postgres@localhost:5432/monitoring
"""
import argparse
import json
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from .alerts import evaluate_windows

SCHEMA = StructType([
    StructField("event_id", StringType()), StructField("event_time", StringType()),
    StructField("service", StringType()), StructField("endpoint", StringType()),
    StructField("region", StringType()), StructField("status", IntegerType()),
    StructField("latency_ms", DoubleType()),
])
WATERMARK = "2 minutes"
WINDOW = "1 minute"


def read_stream(spark: SparkSession, source: str, path: str = None, bootstrap: str = None,
                topic: str = None) -> DataFrame:
    if source == "files":
        raw = spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 10).json(path)
    else:
        kafka = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", bootstrap)
                 .option("subscribe", topic).option("startingOffsets", "latest").load())
        raw = kafka.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e")).select("e.*")
    return raw


def clean(events: DataFrame) -> DataFrame:
    """Parse time, drop malformed events, drop duplicates within the watermark."""
    return (events
            .withColumn("event_ts", F.to_timestamp("event_time"))
            .where(F.col("event_ts").isNotNull() & F.col("service").isNotNull() & F.col("status").isNotNull())
            .withWatermark("event_ts", WATERMARK)
            .dropDuplicates(["event_id", "event_ts"]))


def windowed(events: DataFrame) -> DataFrame:
    """Per-service 1-minute windows. Also usable on a static DataFrame."""
    return (events.groupBy(F.window("event_ts", WINDOW).alias("w"), "service")
            .agg(F.count(F.lit(1)).alias("requests"),
                 F.sum(F.when(F.col("status") >= 500, 1).otherwise(0)).alias("errors"),
                 F.avg("latency_ms").alias("avg_latency_ms"),
                 F.percentile_approx("latency_ms", 0.95, 1000).alias("p95_latency_ms"))
            .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), "service",
                    "requests", "errors", (F.col("errors") / F.col("requests")).alias("error_rate"),
                    F.round("avg_latency_ms", 1).alias("avg_latency_ms"),
                    F.round("p95_latency_ms", 1).alias("p95_latency_ms")))


class Sink:
    """Keeps the latest version of every window and every alert, keyed for upserts.

    Local mode writes JSON Lines files; with --pg it also upserts into Postgres
    tables that the Grafana dashboard reads.
    """

    def __init__(self, out_dir: str, pg_dsn: str | None = None):
        self.out_dir, self.pg = out_dir, pg_dsn
        os.makedirs(out_dir, exist_ok=True)
        self.windows, self.alerts = {}, {}
        if pg_dsn:
            import psycopg
            with psycopg.connect(pg_dsn) as c:
                c.execute(open(os.path.join(os.path.dirname(__file__), "schema.sql")).read())

    def write(self, wins: list[dict], alerts: list[dict]):
        for w in wins:
            self.windows[(w["service"], w["window_start"])] = w
        for a in alerts:
            self.alerts[(a["service"], a["window_start"], a["rule"])] = a
        with open(os.path.join(self.out_dir, "windows.jsonl"), "w") as f:
            f.writelines(json.dumps(w, default=str) + "\n" for w in self.windows.values())
        with open(os.path.join(self.out_dir, "alerts.jsonl"), "w") as f:
            f.writelines(json.dumps(a, default=str) + "\n" for a in self.alerts.values())
        if self.pg and (wins or alerts):
            import psycopg
            with psycopg.connect(self.pg) as c, c.transaction():
                c.cursor().executemany(
                    "INSERT INTO service_windows VALUES (%(window_start)s, %(window_end)s, %(service)s, %(requests)s,"
                    " %(errors)s, %(error_rate)s, %(avg_latency_ms)s, %(p95_latency_ms)s)"
                    " ON CONFLICT (service, window_start) DO UPDATE SET requests = EXCLUDED.requests,"
                    " errors = EXCLUDED.errors, error_rate = EXCLUDED.error_rate,"
                    " avg_latency_ms = EXCLUDED.avg_latency_ms, p95_latency_ms = EXCLUDED.p95_latency_ms", wins)
                c.cursor().executemany(
                    "INSERT INTO alerts VALUES (%(window_start)s, %(service)s, %(rule)s, %(value)s, %(threshold)s)"
                    " ON CONFLICT (service, window_start, rule) DO UPDATE SET value = EXCLUDED.value", alerts)


def run(spark, source, out_dir, path=None, bootstrap=None, topic=None, pg=None, once=False, min_requests=20):
    sink = Sink(out_dir, pg)
    history = {}   # per-service recent windows, for baseline rules

    def process(batch: DataFrame, batch_id: int):
        # Format times inside Spark (session time zone = UTC): collect() would otherwise
        # convert them to the machine's local time zone.
        iso = "yyyy-MM-dd'T'HH:mm:ss'+00:00'"
        batch = (batch.withColumn("window_start", F.date_format("window_start", iso))
                 .withColumn("window_end", F.date_format("window_end", iso)))
        rows = [r.asDict() for r in batch.collect()]          # small: a few rows per service per minute
        alerts = evaluate_windows(rows, history, min_requests=min_requests)
        sink.write(rows, alerts)

    stream = windowed(clean(read_stream(spark, source, path, bootstrap, topic)))
    writer = (stream.writeStream.outputMode("update").foreachBatch(process)
              .option("checkpointLocation", os.path.join(out_dir, "_checkpoint")))
    writer = writer.trigger(availableNow=True) if once else writer.trigger(processingTime="10 seconds")
    query = writer.start()
    query.awaitTermination()
    progress = [p for p in query.recentProgress if p["numInputRows"] > 0]
    stats = {"batches": len(progress), "input_rows": sum(p["numInputRows"] for p in progress),
             "max_processed_rows_per_second": max((p.get("processedRowsPerSecond") or 0) for p in progress) if progress else 0}
    json.dump(stats, open(os.path.join(out_dir, "throughput.json"), "w"), indent=1)
    return stats


def get_spark(kafka: bool = False) -> SparkSession:
    b = (SparkSession.builder.master("local[*]").appName("api-monitoring")
         .config("spark.sql.shuffle.partitions", "4").config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.showConsoleProgress", "false"))
    if kafka:
        b = b.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    return b.getOrCreate()


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", choices=["files", "kafka"], default="files")
    p.add_argument("--path", default="data/events")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="api-requests")
    p.add_argument("--out", default="data/out")
    p.add_argument("--pg", help="Postgres DSN for the Grafana tables")
    p.add_argument("--once", action="store_true", help="Process what's there, then stop")
    args = p.parse_args(argv)
    spark = get_spark(kafka=args.source == "kafka")
    spark.sparkContext.setLogLevel("WARN")
    print(run(spark, args.source, args.out, args.path, args.bootstrap, args.topic, args.pg, args.once))


if __name__ == "__main__":
    main()
