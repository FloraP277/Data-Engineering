import os
import subprocess
import sys

import pytest

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT, "src"))

from hqw import load  # noqa: E402


def test_column_names_are_normalized():
    assert load.snake("City/Town") == "city_town"
    assert load.snake("﻿Facility ID") == "facility_id"
    assert load.snake("Hospital overall rating") == "hospital_overall_rating"


def test_read_rows_keeps_known_columns_and_blanks_become_none():
    text = ("Facility ID,Facility Name,State,Hospital Type,Hospital overall rating,City/Town,Extra\n"
            "010001,General,MD,Acute Care Hospitals,4,Baltimore,x\n"
            "010002,Other,VA,Psychiatric,Not Available,,y\n")
    rows = load.read_rows(text)
    assert rows[0] == {"facility_id": "010001", "facility_name": "General", "state": "MD",
                       "hospital_type": "Acute Care Hospitals", "hospital_overall_rating": "4",
                       "city_town": "Baltimore"}
    assert rows[1]["city_town"] is None


def test_missing_required_column_fails_loudly():
    with pytest.raises(ValueError, match="hospital_overall_rating"):
        load.read_rows("Facility ID,Facility Name,State,Hospital Type\n1,a,MD,x\n")


# ---- full pipeline: needs a Postgres server ---------------------------------
PG = os.environ.get("WAREHOUSE_HOST")
needs_pg = pytest.mark.skipif(not PG, reason="set WAREHOUSE_HOST (and port/user/password) to run against Postgres")


@needs_pg
def test_backfill_builds_scd2_history_and_passes_dbt_tests(tmp_path):
    import psycopg

    from hqw.pipeline import dsn_from_env, ensure_database

    os.environ["WAREHOUSE_DB"] = "warehouse_test"
    dsn = dsn_from_env()
    with psycopg.connect(dsn.rsplit("/", 1)[0] + "/postgres", autocommit=True) as c:
        c.execute("DROP DATABASE IF EXISTS warehouse_test WITH (FORCE)")
    ensure_database(dsn)

    subprocess.run([sys.executable, os.path.join(ROOT, "scripts", "make_sample_data.py"), "--out", str(tmp_path)],
                   check=True, capture_output=True)
    env = {**os.environ, "PYTHONPATH": os.path.join(ROOT, "src")}
    res = subprocess.run([sys.executable, "-m", "hqw.pipeline", "--csv-dir", str(tmp_path)],
                         env=env, capture_output=True, text=True, timeout=900)
    assert res.returncode == 0, res.stdout[-2000:] + res.stderr[-2000:]
    assert "ERROR=0" in res.stdout

    with psycopg.connect(dsn) as c:
        hospitals, versions, current = c.execute(
            "SELECT count(DISTINCT facility_id), count(*), sum(is_current::int) FROM marts.dim_hospital").fetchone()
        assert hospitals == 122            # 120 original + 2 opened in the third release
        assert current == 119              # 3 closed in the third release
        assert versions > hospitals        # rating/ownership changes created new versions
        first = c.execute("SELECT min(valid_from) FROM marts.dim_hospital").fetchone()[0]
        assert str(first) == "2026-01-28"  # history starts at the first release, not the load time
        facts = c.execute("SELECT count(*), count(hospital_key) FROM marts.fct_hospital_ratings").fetchone()
        assert facts == (359, 359)         # 120 + 120 + 119, every row linked to a hospital version
