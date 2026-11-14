"""Where tables live.

`DeltaStore` is used on Databricks: Unity Catalog tables in Delta format,
upserted with MERGE. `ParquetStore` is used for local runs and tests when the
Delta Lake libraries aren't available; it implements the same interface so the
pipeline logic is identical in both places.
"""
import os
import shutil
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass
class UpsertStats:
    table: str
    inserted: int
    updated: int
    unchanged: int

    def as_dict(self):
        return {"table": self.table, "inserted": self.inserted,
                "updated": self.updated, "unchanged": self.unchanged}


def change_stats(spark: SparkSession, existing: DataFrame | None, incoming: DataFrame,
                 keys: list[str], table: str) -> UpsertStats:
    """Count how an upsert will change a table, by comparing row hashes.

    Uses `_row_hash`, which silver computes over every business column, so a
    record only counts as updated when its content actually changed.
    """
    if existing is None:
        return UpsertStats(table, incoming.count(), 0, 0)
    e = existing.select(*keys, F.col("_row_hash").alias("_old_hash"))
    joined = incoming.select(*keys, "_row_hash").join(e, keys, "left")
    counts = joined.agg(
        F.sum(F.when(F.col("_old_hash").isNull(), 1).otherwise(0)).alias("ins"),
        F.sum(F.when(F.col("_old_hash").isNotNull() & (F.col("_old_hash") != F.col("_row_hash")), 1)
              .otherwise(0)).alias("upd"),
        F.sum(F.when(F.col("_old_hash") == F.col("_row_hash"), 1).otherwise(0)).alias("same"),
    ).first()
    return UpsertStats(table, int(counts.ins or 0), int(counts.upd or 0), int(counts.same or 0))


class DeltaStore:
    """Unity Catalog tables, e.g. workspace.mine_safety.silver_mines."""

    def __init__(self, spark: SparkSession, catalog: str, schema: str):
        self.spark = spark
        # Open-source Spark's default catalog (spark_catalog) only takes two-part names
        # in DeltaTable.forName; Unity Catalog on Databricks uses catalog.schema.table.
        self.prefix = schema if catalog == "spark_catalog" else f"{catalog}.{schema}"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.prefix}")

    def name(self, table: str) -> str:
        return f"{self.prefix}.{table}"

    def exists(self, table: str) -> bool:
        return self.spark.catalog.tableExists(self.name(table))

    def read(self, table: str) -> DataFrame:
        return self.spark.table(self.name(table))

    def append(self, df: DataFrame, table: str) -> None:
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(self.name(table))

    def overwrite(self, df: DataFrame, table: str) -> None:
        (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
         .saveAsTable(self.name(table)))

    def upsert(self, df: DataFrame, table: str, keys: list[str]) -> UpsertStats:
        from delta.tables import DeltaTable

        if not self.exists(table):
            stats = change_stats(self.spark, None, df, keys, table)
            self.overwrite(df, table)
            return stats
        stats = change_stats(self.spark, self.read(table), df, keys, table)
        cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        (DeltaTable.forName(self.spark, self.name(table)).alias("t")
         .merge(df.alias("s"), cond)
         .whenMatchedUpdateAll(condition="t._row_hash <> s._row_hash")
         .whenNotMatchedInsertAll()
         .execute())
        return stats


class ParquetStore:
    """Parquet folders under a local root, for development and tests."""

    def __init__(self, spark: SparkSession, root: str):
        self.spark = spark
        self.root = root
        os.makedirs(root, exist_ok=True)

    def name(self, table: str) -> str:
        return os.path.join(self.root, table)

    def exists(self, table: str) -> bool:
        return os.path.exists(os.path.join(self.name(table), "_SUCCESS"))

    def read(self, table: str) -> DataFrame:
        return self.spark.read.parquet(self.name(table))

    def append(self, df: DataFrame, table: str) -> None:
        df.write.mode("append").parquet(self.name(table))

    def overwrite(self, df: DataFrame, table: str) -> None:
        # Write to a temp folder first: Spark reads lazily, so overwriting a
        # folder that the DataFrame is still reading from would lose data.
        tmp = self.name(table) + "__tmp"
        df.write.mode("overwrite").parquet(tmp)
        shutil.rmtree(self.name(table), ignore_errors=True)
        os.rename(tmp, self.name(table))

    def upsert(self, df: DataFrame, table: str, keys: list[str]) -> UpsertStats:
        if not self.exists(table):
            stats = change_stats(self.spark, None, df, keys, table)
            self.overwrite(df, table)
            return stats
        existing = self.read(table)
        stats = change_stats(self.spark, existing, df, keys, table)
        # Same result as the Delta MERGE: only new rows and rows whose hash
        # changed are written; unchanged rows keep their original values.
        old = existing.select(*keys, F.col("_row_hash").alias("_old_hash"))
        changed = (df.join(old, keys, "left")
                   .where(F.col("_old_hash").isNull() | (F.col("_old_hash") != F.col("_row_hash")))
                   .drop("_old_hash"))
        kept = existing.join(changed.select(*keys), keys, "left_anti")
        self.overwrite(kept.unionByName(changed, allowMissingColumns=True), table)
        return stats
