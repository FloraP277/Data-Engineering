"""Landing and bronze: get the raw files and store them exactly as received."""
import os
import urllib.request
import zipfile

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Dataset


def download(dataset: Dataset, landing_dir: str) -> str:
    """Download a dataset's zip from MSHA into `landing_dir` and unzip it.

    Returns the path of the extracted .txt file. Used on Databricks, where
    `landing_dir` is a Unity Catalog volume such as
    /Volumes/workspace/mine_safety/landing/2026-11-06.
    """
    os.makedirs(landing_dir, exist_ok=True)
    zip_path = os.path.join(landing_dir, dataset.zip_name)
    req = urllib.request.Request(dataset.url, headers={"User-Agent": "mine-safety-lakehouse"})
    with urllib.request.urlopen(req, timeout=300) as resp, open(zip_path, "wb") as out:
        while chunk := resp.read(1 << 20):
            out.write(chunk)
    return extract(zip_path, landing_dir)


def extract(zip_path: str, out_dir: str) -> str:
    """Unzip an MSHA archive and return the path of the single .txt inside."""
    with zipfile.ZipFile(zip_path) as zf:
        txt = [n for n in zf.namelist() if n.lower().endswith(".txt")]
        if len(txt) != 1:
            raise ValueError(f"{zip_path}: expected one .txt file, found {txt}")
        zf.extract(txt[0], out_dir)
        return os.path.join(out_dir, txt[0])


def _clean_name(col: str) -> str:
    # Headers can carry a byte-order mark or stray spaces.
    return col.replace("﻿", "").strip().upper()


def read_raw(spark: SparkSession, path: str, dataset: Dataset) -> DataFrame:
    """Read a pipe-delimited MSHA file with every column as a string.

    Bronze keeps raw text on purpose: type conversion happens in silver, where
    bad values can be caught and reported instead of silently becoming nulls.
    """
    df = (spark.read
          .option("header", True)
          .option("sep", "|")
          .option("encoding", "ISO-8859-1")   # MSHA files are not UTF-8
          .option("quote", "\u0000")          # narratives contain stray quotes
          .option("inferSchema", False)
          .csv(path))
    df = df.toDF(*[_clean_name(c) for c in df.columns])
    missing = [c for c in dataset.required if c not in df.columns]
    if missing:
        raise ValueError(
            f"{dataset.name}: file is missing required columns {missing}. "
            f"Columns found: {df.columns}. Check MSHA's definition file and update config.py."
        )
    return df


def to_bronze(df: DataFrame, source_file: str, load_date: str) -> DataFrame:
    """Add lineage columns: which file each row came from and when."""
    return (df
            .withColumn("_source_file", F.lit(os.path.basename(source_file)))
            .withColumn("_load_date", F.lit(load_date).cast("date"))
            .withColumn("_ingested_at", F.current_timestamp()))


def load_bronze(store, dataset: Dataset, df: DataFrame, load_date: str) -> int:
    """Append a batch to bronze, once per load date.

    Re-running the same load date is a no-op, so a retried job never
    duplicates raw data.
    """
    table = f"bronze_{dataset.name}"
    if store.exists(table):
        already = store.read(table).where(F.col("_load_date") == F.lit(load_date).cast("date")).limit(1).count()
        if already:
            print(f"[bronze] {table}: load {load_date} already present, skipping")
            return 0
    rows = df.count()
    store.append(df, table)
    print(f"[bronze] {table}: appended {rows:,} rows for {load_date}")
    return rows
