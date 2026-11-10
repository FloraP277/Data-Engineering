"""Download CMS Hospital General Information and load it into a raw Postgres table.

Each load is one snapshot, stored with its snapshot date. Loading the same date
again replaces that snapshot instead of duplicating it.

    python -m hqw.load --snapshot-date 2026-11-06                 # download from CMS
    python -m hqw.load --snapshot-date 2026-11-06 --csv file.csv  # load a file you have
"""
import argparse
import csv
import io
import json
import os
import re
import urllib.request

import psycopg

# CMS Provider Data Catalog: Hospital General Information
CMS_METASTORE = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/xubh-q36u"

REQUIRED = ["facility_id", "facility_name", "state", "hospital_type", "hospital_overall_rating"]
OPTIONAL = ["address", "city_town", "zip_code", "county_parish", "hospital_ownership", "emergency_services"]


def snake(name: str) -> str:
    """'City/Town' -> 'city_town', 'Hospital overall rating' -> 'hospital_overall_rating'."""
    return re.sub(r"[^a-z0-9]+", "_", name.replace("﻿", "").strip().lower()).strip("_")


def download_url() -> str:
    """Ask CMS's catalog for the current CSV link (it changes with each release)."""
    with urllib.request.urlopen(CMS_METASTORE, timeout=60) as r:
        meta = json.load(r)
    return next(d["downloadURL"] for d in meta["distribution"] if d.get("downloadURL", "").endswith(".csv"))


def read_rows(text: str) -> list[dict]:
    reader = csv.DictReader(io.StringIO(text))
    cols = {snake(c): c for c in reader.fieldnames or []}
    missing = [c for c in REQUIRED if c not in cols]
    if missing:
        raise ValueError(f"CSV is missing required columns {missing}. Found: {sorted(cols)}")
    keep = [c for c in REQUIRED + OPTIONAL if c in cols]
    return [{c: (row[cols[c]] or "").strip() or None for c in keep} for row in reader]


DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.hospital_general_information (
    facility_id text, facility_name text, address text, city_town text, state text, zip_code text,
    county_parish text, hospital_type text, hospital_ownership text, emergency_services text,
    hospital_overall_rating text,
    snapshot_date date NOT NULL, source text, loaded_at timestamptz DEFAULT now()
);
"""
COLS = ["facility_id", "facility_name", "address", "city_town", "state", "zip_code", "county_parish",
        "hospital_type", "hospital_ownership", "emergency_services", "hospital_overall_rating"]


def load(dsn: str, rows: list[dict], snapshot_date: str, source: str) -> int:
    with psycopg.connect(dsn) as conn, conn.transaction():
        conn.execute(DDL)
        conn.execute("DELETE FROM raw.hospital_general_information WHERE snapshot_date = %s", (snapshot_date,))
        with conn.cursor().copy(
                f"COPY raw.hospital_general_information ({', '.join(COLS)}, snapshot_date, source) FROM STDIN") as cp:
            for r in rows:
                cp.write_row([r.get(c) for c in COLS] + [snapshot_date, source])
    return len(rows)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--snapshot-date", required=True)
    p.add_argument("--csv", help="Load this file instead of downloading")
    p.add_argument("--dsn", default=os.environ.get("WAREHOUSE_DSN", "postgresql://postgres:postgres@localhost:5432/warehouse"))
    args = p.parse_args(argv)
    if args.csv:
        text, source = open(args.csv, encoding="utf-8-sig").read(), os.path.basename(args.csv)
    else:
        source = download_url()
        with urllib.request.urlopen(source, timeout=300) as r:
            text = r.read().decode("utf-8-sig")
    n = load(args.dsn, read_rows(text), args.snapshot_date, source)
    print(f"loaded {n:,} hospitals for snapshot {args.snapshot_date}")


if __name__ == "__main__":
    main()
