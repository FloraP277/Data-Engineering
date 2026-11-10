"""Integration tests against PostgreSQL.

Set DIRSYNC_TEST_DSN to a server where the tests may create databases, e.g.
    DIRSYNC_TEST_DSN=postgresql://postgres:postgres@localhost:5432/postgres pytest
CI does this with a Postgres service container.
"""
import datetime as dt
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

ADMIN_DSN = os.environ.get("DIRSYNC_TEST_DSN")
pytestmark = pytest.mark.skipif(not ADMIN_DSN, reason="DIRSYNC_TEST_DSN not set")

D = dt.date(2026, 11, 2)
T = datetime(2026, 11, 2, 9, 0, tzinfo=timezone.utc)


@pytest.fixture
def conn():
    import psycopg

    name = f"dirsync_test_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(ADMIN_DSN, autocommit=True) as admin:
        admin.execute(f"CREATE DATABASE {name}")
    dsn = ADMIN_DSN.rsplit("/", 1)[0] + f"/{name}"
    c = psycopg.connect(dsn)
    yield c
    c.close()
    with psycopg.connect(ADMIN_DSN, autocommit=True) as admin:
        admin.execute(f"DROP DATABASE {name} WITH (FORCE)")


def seed(conn, employees, accounts):
    from dirsync.simulate import ACCT_COLS, EMP_COLS, SOURCE_DDL

    with conn.cursor() as cur:
        cur.execute(SOURCE_DDL)
        for e in employees:
            row = {"title": "Analyst", "hire_date": D - timedelta(days=100), "termination_date": None,
                   "status": "active", "department": "Finance", "updated_at": T, **e}
            cur.execute(f"INSERT INTO hr.employees ({', '.join(EMP_COLS)}) VALUES ({', '.join(['%s'] * len(EMP_COLS))})",
                        [row[c] for c in EMP_COLS])
        for a in accounts:
            row = {"enabled": True, "department": "Finance", "created_at": T, "updated_at": T, **a}
            cur.execute(f"INSERT INTO directory.accounts ({', '.join(ACCT_COLS)}) VALUES ({', '.join(['%s'] * len(ACCT_COLS))})",
                        [row[c] for c in ACCT_COLS])
    conn.commit()


def emp(eid, first="Ann", last="Lee", **kw):
    return {"employee_id": eid, "first_name": first, "last_name": last, "email": f"{first}.{last}.{eid}@example.com".lower(), **kw}


def acct(aid, eid, first="Ann", last="Lee", **kw):
    return {"account_id": aid, "employee_id": eid, "upn": f"{first}.{last}.{eid}@example.com".lower(),
            "display_name": f"{first} {last}", **kw}


def test_reconciliation_finds_every_kind_of_mismatch(conn):
    from dirsync.reconcile import issues_for
    from dirsync.run import run

    seed(conn,
         [emp("E1"), emp("E2"), emp("E3", status="terminated", termination_date=D), emp("E4"),
          emp("E5"), emp("E6", department="Legal"), emp("E7", first="Bo", last="Ng")],
         [acct("A1", "E1"),                                         # clean
          # E2 has no account                                       -> missing_account
          acct("A3", "E3"),                                         # -> leaver_still_active
          acct("A4", "E4"), acct("A4b", "E4", created_at=T + timedelta(hours=1)),   # -> duplicate_account
          acct("A5", "E5", display_name="Ann Smith"),               # -> name_mismatch
          acct("A6", "E6"),                                         # Finance vs Legal -> department_mismatch
          acct("A7", None, first="Bo", last="Ng", upn="bo.ng.e7@example.com"),       # matched by email: clean
          acct("A8", None, upn="contractor9@example.com"),          # -> orphan_account
          acct("A9", "E1", enabled=False)])                         # disabled: ignored
    res = run(conn, D)
    found = {(i["issue_type"], i["employee_id"], i["account_id"]) for i in issues_for(conn, D)}
    assert found == {("missing_account", "E2", None), ("leaver_still_active", "E3", "A3"),
                     ("duplicate_account", "E4", "A4b"), ("name_mismatch", "E5", "A5"),
                     ("department_mismatch", "E6", "A6"), ("orphan_account", None, "A8")}
    rec = res["reconciliation"]
    assert rec["active_employees"] == 6 and rec["clean_employees"] == 2   # E1 and E7
    assert rec["accuracy"] == pytest.approx(2 / 6, abs=1e-4)


def test_incremental_sync_watermark_overlap_and_idempotence(conn):
    from dirsync.run import run

    seed(conn, [emp("E1"), emp("E2")], [acct("A1", "E1"), acct("A2", "E2")])
    first = run(conn, D)["sync"]
    assert first["hr"]["inserted"] == 2 and first["directory"]["inserted"] == 2

    again = run(conn, D + timedelta(days=1))["sync"]
    assert again["hr"]["inserted"] == 0 and again["hr"]["updated"] == 0      # nothing changed
    assert again["hr"]["unchanged"] == 2                                     # overlap re-read, not rewritten

    with conn.cursor() as cur:
        cur.execute("UPDATE hr.employees SET last_name = 'Park', updated_at = %s WHERE employee_id = 'E1'",
                    (T + timedelta(days=1),))
        # A late commit: timestamp slightly BEFORE the watermark, inside the 5-minute overlap.
        cur.execute("UPDATE hr.employees SET department = 'Legal', updated_at = %s WHERE employee_id = 'E2'",
                    (T + timedelta(days=1) - timedelta(minutes=2),))
    conn.commit()
    third = run(conn, D + timedelta(days=2))
    assert third["sync"]["hr"]["updated"] == 2
    assert third["reconciliation"]["issues"]["name_mismatch"] == 1
    assert third["reconciliation"]["issues"]["department_mismatch"] == 1


def test_error_check_stops_the_run_before_reconciliation(conn):
    from dirsync.checks import ChecksFailed
    from dirsync.run import run

    seed(conn, [emp("E1"), emp("E2", email="")], [acct("A1", "E1")])
    with pytest.raises(ChecksFailed, match="hr_active_missing_email"):
        run(conn, D)
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('sync.reconciliation_summary')")
        assert cur.fetchone()[0] is None                                    # reconcile never ran
        cur.execute("SELECT failed FROM sync.check_results WHERE check_name = 'hr_active_missing_email'")
        assert cur.fetchone()[0] == 1


def test_simulated_month_counts_every_change_exactly(conn, tmp_path):
    from dirsync.demo import plot, simulate_days

    df = simulate_days(conn, employees=800, days=10, start=dt.date(2026, 10, 1), fix_rate=0.3, seed=3)
    assert len(df) == 10
    assert (df["hr_synced"] == df["hr_changes"]).all()
    # directory changes each day = HR-driven changes + the previous evening's IT fixes
    expected = df["directory_changes"] + df["fixed_by_it"].shift(1, fill_value=0)
    assert (df["directory_synced"] == expected).all()
    assert df["accuracy"].between(0.95, 1.0).all()
    assert df["fixed_by_it"].sum() > 0
    plot(df, str(tmp_path / "accuracy.png"))
    assert (tmp_path / "accuracy.png").stat().st_size > 0
