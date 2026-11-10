"""Load one or more CMS releases in date order and run dbt.

For each release: load raw rows -> build staging -> dbt snapshot (records changes
as SCD Type 2). Then build the star schema and run every dbt test.

    # Backfill from files you already have (sorted by the date in the file name)
    python -m hqw.pipeline --csv-dir sample_data
    # One new release downloaded from CMS
    python -m hqw.pipeline --download --snapshot-date 2026-11-06
"""
import argparse
import glob
import json
import os
import re
import subprocess
import sys

import psycopg

from . import load

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DBT_DIR = os.path.join(ROOT, "dbt")


def dsn_from_env() -> str:
    e = os.environ.get
    return (f"postgresql://{e('WAREHOUSE_USER', 'postgres')}:{e('WAREHOUSE_PASSWORD', 'postgres')}"
            f"@{e('WAREHOUSE_HOST', 'localhost')}:{e('WAREHOUSE_PORT', '5432')}/{e('WAREHOUSE_DB', 'warehouse')}")


def ensure_database(dsn: str):
    """Create the warehouse database if it doesn't exist yet."""
    db = dsn.rsplit("/", 1)[1]
    with psycopg.connect(dsn.rsplit("/", 1)[0] + "/postgres", autocommit=True) as c:
        if not c.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db,)).fetchone():
            c.execute(f'CREATE DATABASE "{db}"')


def dbt(*args, vars: dict | None = None):
    cmd = [sys.executable, "-m", "dbt.cli.main", *args, "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR]
    if vars:
        cmd += ["--vars", json.dumps(vars)]
    print("$ dbt " + " ".join(args))
    res = subprocess.run(cmd, capture_output=True, text=True)
    print(res.stdout[-3000:])
    if res.returncode != 0:
        raise RuntimeError(f"dbt {' '.join(args)} failed:\n{res.stdout[-3000:]}\n{res.stderr[-2000:]}")
    return res.stdout


def snapshot_release(dsn: str, rows: list, snapshot_date: str, source: str):
    n = load.load(dsn, rows, snapshot_date, source)
    print(f"loaded {n:,} hospitals for {snapshot_date}")
    dbt("run", "--select", "staging", vars={"snapshot_date": snapshot_date})
    dbt("snapshot", vars={"snapshot_date": snapshot_date})


def build_and_test():
    dbt("run", "--select", "marts")
    return dbt("test")


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--csv-dir")
    p.add_argument("--download", action="store_true")
    p.add_argument("--snapshot-date")
    args = p.parse_args(argv)

    dsn = dsn_from_env()
    ensure_database(dsn)
    if args.csv_dir:
        files = sorted(glob.glob(os.path.join(args.csv_dir, "*.csv")),
                       key=lambda f: re.search(r"\d{4}-\d{2}-\d{2}", f).group(0))
        for f in files:
            day = re.search(r"\d{4}-\d{2}-\d{2}", f).group(0)
            snapshot_release(dsn, load.read_rows(open(f, encoding="utf-8-sig").read()), day, os.path.basename(f))
    elif args.download:
        if not args.snapshot_date:
            p.error("--snapshot-date is required with --download")
        url = load.download_url()
        import urllib.request
        with urllib.request.urlopen(url, timeout=300) as r:
            rows = load.read_rows(r.read().decode("utf-8-sig"))
        snapshot_release(dsn, rows, args.snapshot_date, url)
    else:
        p.error("use --csv-dir or --download")
    build_and_test()


if __name__ == "__main__":
    main()
