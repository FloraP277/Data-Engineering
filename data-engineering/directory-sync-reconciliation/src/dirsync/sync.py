"""Incremental change data capture from both sources into the sync schema.

Each source has a watermark: the latest `updated_at` already loaded. A run reads
only rows changed since then, minus a small overlap window, so a row committed
slightly late with an older timestamp isn't missed. Re-reading overlap rows is
harmless: a hash of each row's business columns means unchanged rows are
counted as unchanged and not rewritten.
"""
import time
from datetime import timedelta

SYNC_DDL = """
CREATE SCHEMA IF NOT EXISTS sync;
CREATE TABLE IF NOT EXISTS sync.hr_employees (
    employee_id text PRIMARY KEY, first_name text, last_name text, email text, department text, title text,
    status text, hire_date date, termination_date date, updated_at timestamptz,
    row_hash text NOT NULL, synced_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS sync.dir_accounts (
    account_id text PRIMARY KEY, employee_id text, upn text, display_name text, department text,
    enabled boolean, created_at timestamptz, updated_at timestamptz,
    row_hash text NOT NULL, synced_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS sync.watermarks (source text PRIMARY KEY, last_updated_at timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS sync.runs (
    run_date date, source text, pulled int, inserted int, updated int, unchanged int, seconds numeric,
    PRIMARY KEY (run_date, source)
);
"""

SOURCES = {
    "hr": ("hr.employees", "sync.hr_employees", "employee_id",
           ["employee_id", "first_name", "last_name", "email", "department", "title", "status",
            "hire_date", "termination_date"]),
    "directory": ("directory.accounts", "sync.dir_accounts", "account_id",
                  ["account_id", "employee_id", "upn", "display_name", "department", "enabled", "created_at"]),
}


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(SYNC_DDL)
    conn.commit()


def pull(conn, source: str, run_date, overlap: timedelta = timedelta(minutes=5)) -> dict:
    src, tgt, key, cols = SOURCES[source]
    start = time.perf_counter()
    row_hash = "md5(concat_ws('|', " + ", ".join(f"coalesce({c}::text, '<null>')" for c in cols) + "))"
    with conn.cursor() as cur:
        cur.execute("SELECT last_updated_at FROM sync.watermarks WHERE source = %s", (source,))
        row = cur.fetchone()
        wm = row[0] if row else None
        where, params = ("WHERE updated_at >= %s", (wm - overlap,)) if wm else ("", ())
        cur.execute(f"CREATE TEMP TABLE batch ON COMMIT DROP AS "
                    f"SELECT {', '.join(cols)}, updated_at, {row_hash} AS row_hash FROM {src} {where}", params)
        cur.execute(f"SELECT count(*), count(*) FILTER (WHERE t.{key} IS NULL),"
                    f" count(*) FILTER (WHERE t.row_hash <> b.row_hash), max(b.updated_at)"
                    f" FROM batch b LEFT JOIN {tgt} t USING ({key})")
        pulled, inserted, updated, max_ts = cur.fetchone()
        all_cols = cols + ["updated_at", "row_hash"]
        sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in all_cols if c != key)
        cur.execute(f"INSERT INTO {tgt} ({', '.join(all_cols)}, synced_at)"
                    f" SELECT {', '.join(all_cols)}, now() FROM batch"
                    f" ON CONFLICT ({key}) DO UPDATE SET {sets}, synced_at = now()"
                    f" WHERE {tgt}.row_hash <> EXCLUDED.row_hash")
        if max_ts is not None and (wm is None or max_ts > wm):
            cur.execute("INSERT INTO sync.watermarks VALUES (%s, %s)"
                        " ON CONFLICT (source) DO UPDATE SET last_updated_at = EXCLUDED.last_updated_at",
                        (source, max_ts))
        stats = {"pulled": pulled, "inserted": inserted, "updated": updated,
                 "unchanged": pulled - inserted - updated, "seconds": round(time.perf_counter() - start, 3)}
        cur.execute("INSERT INTO sync.runs VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    " ON CONFLICT (run_date, source) DO UPDATE SET pulled = EXCLUDED.pulled,"
                    " inserted = EXCLUDED.inserted, updated = EXCLUDED.updated, unchanged = EXCLUDED.unchanged,"
                    " seconds = EXCLUDED.seconds",
                    (run_date, source, pulled, inserted, updated, stats["unchanged"], stats["seconds"]))
    conn.commit()
    return stats
