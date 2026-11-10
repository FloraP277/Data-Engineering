"""Airflow DAG: check CMS for a new Hospital General Information release every
Monday; if there is one, load it, snapshot it (SCD Type 2), rebuild the star
schema and run the dbt tests.
"""
import os
import sys
from datetime import datetime

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

sys.path.insert(0, os.environ.get("HQW_SRC", "/opt/airflow/project/src"))


@dag(schedule="0 7 * * 1", start_date=datetime(2026, 1, 1), catchup=False, max_active_runs=1,
     tags=["cms", "dbt"], default_args={"retries": 2})
def hospital_quality():

    @task
    def find_new_release() -> str:
        """Return the CSV URL if it hasn't been loaded before; otherwise skip the run."""
        import psycopg

        from hqw import load
        from hqw.pipeline import dsn_from_env, ensure_database

        dsn = dsn_from_env()
        ensure_database(dsn)
        url = load.download_url()
        with psycopg.connect(dsn) as c:
            c.execute(load.DDL)
            seen = c.execute("SELECT 1 FROM raw.hospital_general_information WHERE source = %s LIMIT 1",
                             (url,)).fetchone()
        if seen:
            raise AirflowSkipException(f"already loaded {url}")
        return url

    @task
    def load_and_snapshot(url: str, ds=None) -> int:
        import urllib.request

        from hqw import load
        from hqw.pipeline import dsn_from_env, snapshot_release

        with urllib.request.urlopen(url, timeout=300) as r:
            rows = load.read_rows(r.read().decode("utf-8-sig"))
        snapshot_release(dsn_from_env(), rows, ds, url)
        return len(rows)

    @task
    def build_star_schema_and_test():
        from hqw.pipeline import build_and_test
        build_and_test()

    load_and_snapshot(find_new_release()) >> build_star_schema_and_test()


hospital_quality()
