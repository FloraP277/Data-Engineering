"""Run the lakehouse end to end: landing -> bronze -> silver -> gold.

Databricks (Delta tables in Unity Catalog, downloads from MSHA):
    python -m lakehouse.pipeline --mode databricks --catalog workspace --schema mine_safety

Local (Parquet tables, zips you already have, e.g. the sample data):
    python -m lakehouse.pipeline --mode local --source-dir sample_data/2026-10-30 --root ./lakehouse_data
"""
import argparse
import datetime as dt
import glob
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import DATASETS
from .gold import build_gold
from .ingest import download, extract, load_bronze, read_raw, to_bronze
from .silver import build_silver
from .store import DeltaStore, ParquetStore

# Mines must load first: the other datasets check their MINE_ID against it.
ORDER = ["mines", "inspections", "violations", "accidents"]


def get_spark(app_name: str = "mine-safety-lakehouse") -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:          # Databricks notebooks and jobs
        return active
    return (SparkSession.builder.appName(app_name).master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate())


def run(store, files: dict[str, str], load_date: str, spark: SparkSession) -> dict:
    """Load one batch. `files` maps dataset name -> extracted .txt path."""
    summary = {"load_date": load_date, "bronze": {}, "silver": {}}
    for name in ORDER:
        ds = DATASETS[name]
        raw = to_bronze(read_raw(spark, files[name], ds), files[name], load_date)
        summary["bronze"][name] = load_bronze(store, ds, raw, load_date)
        summary["silver"][name] = build_silver(store, ds, load_date)
    summary["gold"] = build_gold(store)

    audit = spark.createDataFrame(
        [(load_date, name, json.dumps(summary["silver"][name])) for name in ORDER],
        "load_date string, dataset string, silver_result string",
    ).withColumn("load_date", F.col("load_date").cast("date")).withColumn("run_at", F.current_timestamp())
    store.append(audit, "load_audit")
    return summary


def local_files(source_dir: str, work_dir: str) -> dict[str, str]:
    """Extract the four MSHA zips found in `source_dir`."""
    files = {}
    for name in ORDER:
        zip_name = DATASETS[name].zip_name
        matches = glob.glob(os.path.join(source_dir, zip_name))
        if not matches:
            raise FileNotFoundError(f"{zip_name} not found in {source_dir}")
        files[name] = extract(matches[0], work_dir)
    return files


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mode", choices=["databricks", "local"], required=True)
    p.add_argument("--load-date", default=dt.date.today().isoformat(),
                   help="Batch date, YYYY-MM-DD. Defaults to today.")
    p.add_argument("--catalog", default="workspace")
    p.add_argument("--schema", default="mine_safety")
    p.add_argument("--volume", default="landing", help="Unity Catalog volume for raw files")
    p.add_argument("--source-dir", help="Local mode: folder containing the MSHA zips")
    p.add_argument("--root", default="./lakehouse_data", help="Local mode: where tables are written")
    args = p.parse_args(argv)

    spark = get_spark()
    if args.mode == "databricks":
        store = DeltaStore(spark, args.catalog, args.schema)
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {args.catalog}.{args.schema}.{args.volume}")
        landing = f"/Volumes/{args.catalog}/{args.schema}/{args.volume}/{args.load_date}"
        files = {name: download(DATASETS[name], landing) for name in ORDER}
    else:
        if not args.source_dir:
            p.error("--source-dir is required in local mode")
        store = ParquetStore(spark, args.root)
        files = local_files(args.source_dir, os.path.join(args.root, "_landing", args.load_date))

    summary = run(store, files, args.load_date, spark)
    print(json.dumps(summary, indent=2, default=str))
    return summary


if __name__ == "__main__":
    main()
