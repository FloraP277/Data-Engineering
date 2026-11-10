"""Silver: typed, cleaned, de-duplicated and quality-checked records."""
from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .config import Dataset
from .quality import ORPHAN_RULE, RULES, Rule
from .store import UpsertStats

DATE_FORMATS = ("MM/dd/yyyy", "yyyy-MM-dd", "MM/dd/yyyy HH:mm:ss", "yyyy-MM-dd HH:mm:ss")


def parse_date(col: str):
    """Try each known date format; return NULL instead of failing on bad values.

    try_to_timestamp keeps this safe when Spark runs in ANSI mode (the default
    on Databricks serverless), where a plain to_date would crash the job.
    """
    attempts = [F.expr(f"try_to_timestamp(`{col}`, '{fmt}')") for fmt in DATE_FORMATS]
    return F.to_date(F.coalesce(*attempts))


def standardize(df: DataFrame, dataset: Dataset) -> tuple[DataFrame, list[Rule]]:
    """Trim text, turn blanks into NULL, normalize IDs and cast types.

    Returns the typed DataFrame plus one `warn` rule per typed column, so values
    that could not be converted are counted rather than silently lost.
    """
    raw_cols = [c for c in df.columns if not c.startswith("_")]
    for c in raw_cols:
        df = df.withColumn(c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))))

    # MSHA mine IDs are 7 digits with leading zeros; some exports drop them.
    if "MINE_ID" in df.columns:
        df = df.withColumn(
            "MINE_ID",
            F.when(F.col("MINE_ID").rlike("^[0-9]{1,7}$"), F.lpad("MINE_ID", 7, "0"))
             .otherwise(F.col("MINE_ID")))

    cast_rules = []
    typed = (
        [(c, parse_date(c), "invalid_date") for c in dataset.date_cols]
        + [(c, F.expr(f"try_cast(`{c}` AS INT)"), "invalid_integer") for c in dataset.int_cols]
        + [(c, F.expr(f"try_cast(`{c}` AS DECIMAL(12,2))"), "invalid_decimal") for c in dataset.decimal_cols]
    )
    for c, expr, label in typed:
        if c not in df.columns:
            continue
        df = df.withColumn(f"__raw_{c}", F.col(c)).withColumn(c, expr)
        cast_rules.append(Rule(f"{label}:{c}", f"`__raw_{c}` IS NOT NULL AND `{c}` IS NULL", "warn"))
    return df, cast_rules


def add_row_hash(df: DataFrame, cols: list[str]) -> DataFrame:
    """Fingerprint of a row's content, used to detect changed records."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols]
    return df.withColumn("_row_hash", F.sha2(F.concat_ws("||", *parts), 256))


def deduplicate(df: DataFrame, keys: list[str]) -> tuple[DataFrame, int]:
    """Keep one row per key. Exact duplicates collapse; conflicting duplicates
    keep the row with the highest hash, so the result is the same on every run."""
    w = Window.partitionBy(*keys).orderBy(F.col("_row_hash").desc())
    ranked = df.withColumn("_rn", F.row_number().over(w))
    dropped = ranked.where("_rn > 1").count()
    return ranked.where("_rn = 1").drop("_rn"), dropped


def apply_rules(df: DataFrame, rules: list[Rule]) -> DataFrame:
    """Add arrays listing the error and warning rules each row fails."""
    def failing(sev):
        checks = [F.when(F.expr(r.fails_when), F.lit(r.name)) for r in rules if r.severity == sev]
        if not checks:
            return F.array().cast("array<string>")
        return F.filter(F.array(*checks), lambda x: x.isNotNull())

    return df.withColumn("_dq_errors", failing("error")).withColumn("_dq_warnings", failing("warn"))


def dq_summary(df: DataFrame, rules: list[Rule], dataset: str, load_date: str) -> DataFrame:
    """One row per rule: how many records failed it in this batch."""
    total = F.count(F.lit(1))
    aggs = [F.sum(F.when(F.expr(r.fails_when), 1).otherwise(0)).alias(f"r{i}") for i, r in enumerate(rules)]
    row = df.agg(total.alias("total"), *aggs).first()
    data = [(dataset, load_date, r.name, r.severity, int(row[f"r{i}"] or 0), int(row["total"]))
            for i, r in enumerate(rules)]
    schema = "dataset string, load_date string, rule string, severity string, failed long, total long"
    return (df.sparkSession.createDataFrame(data, schema)
            .withColumn("load_date", F.col("load_date").cast("date"))
            .withColumn("failed_pct", F.round(F.col("failed") / F.greatest(F.col("total"), F.lit(1)) * 100, 3)))


def replace_batch(store, df: DataFrame, table: str, dataset: str, load_date: str) -> None:
    """Write a batch's rows, replacing any earlier rows for the same dataset and
    load date, so re-running a load never doubles the quarantine or DQ log."""
    if store.exists(table):
        existing = store.read(table).where(
            ~((F.col("dataset") == dataset) & (F.col("load_date") == F.lit(load_date).cast("date"))))
        df = existing.unionByName(df, allowMissingColumns=True)
    store.overwrite(df, table)


def build_silver(store, dataset: Dataset, load_date: str) -> dict:
    """Bronze batch -> standardized, checked silver rows (+ quarantine, DQ log)."""
    bronze = store.read(f"bronze_{dataset.name}").where(
        F.col("_load_date") == F.lit(load_date).cast("date"))
    df, cast_rules = standardize(bronze, dataset)

    keep = [c for c in dataset.keep if c in df.columns]
    df = add_row_hash(df, keep)
    df, dup_dropped = deduplicate(df, list(dataset.keys))

    rules = list(RULES[dataset.name]) + cast_rules
    if dataset.name != "mines" and store.exists("silver_mines"):
        mines = store.read("silver_mines").select("MINE_ID", F.lit(1).alias("_mine_found"))
        df = df.join(mines, "MINE_ID", "left")
        rules.append(ORPHAN_RULE)

    df = apply_rules(df, rules)
    replace_batch(store, dq_summary(df, rules, dataset.name, load_date),
                  "dq_results", dataset.name, load_date)

    bad = (df.where(F.size("_dq_errors") > 0)
           .select(*keep, "_dq_errors", "_source_file")
           .withColumn("dataset", F.lit(dataset.name))
           .withColumn("load_date", F.lit(load_date).cast("date")))
    n_bad = bad.count()
    replace_batch(store, bad, f"quarantine_{dataset.name}", dataset.name, load_date)

    good = (df.where(F.size("_dq_errors") == 0)
            .select(*keep, "_row_hash", F.col("_load_date")))
    stats: UpsertStats = store.upsert(good, f"silver_{dataset.name}", list(dataset.keys))
    result = {**stats.as_dict(), "quarantined": n_bad, "duplicates_dropped": dup_dropped}
    print(f"[silver] {result}")
    return result
