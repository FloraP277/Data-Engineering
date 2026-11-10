"""One daily run: incremental sync of both sources -> quality checks -> reconciliation.

    DIRSYNC_DSN=postgresql://... python -m dirsync.run --run-date 2026-11-07

Exits with status 1 if an error-level check fails, so a scheduler (Kubernetes
CronJob, Airflow) marks the run as failed and the bad data is never reconciled.
"""
import argparse
import datetime as dt
import json
import os
import sys

import psycopg

from .checks import ChecksFailed, run_checks
from .reconcile import reconcile
from .sync import ensure_schema, pull


def run(conn, run_date) -> dict:
    ensure_schema(conn)
    result = {"run_date": str(run_date), "sync": {s: pull(conn, s, run_date) for s in ("hr", "directory")}}
    result["checks"] = run_checks(conn, run_date)
    failed = [c for c in result["checks"] if c["severity"] == "error" and c["failed"]]
    if failed:
        raise ChecksFailed(f"error-level checks failed: {failed}")
    result["reconciliation"] = reconcile(conn, run_date)
    return result


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-date", default=dt.date.today().isoformat())
    p.add_argument("--dsn", default=os.environ.get("DIRSYNC_DSN"))
    args = p.parse_args(argv)
    if not args.dsn:
        p.error("set DIRSYNC_DSN or pass --dsn")
    with psycopg.connect(args.dsn) as conn:
        try:
            print(json.dumps(run(conn, dt.date.fromisoformat(args.run_date)), indent=2, default=str))
        except ChecksFailed as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
