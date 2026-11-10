"""Compare the synced HR records with the synced directory accounts.

Accounts are linked to employees by employee_id, or by email when the account
has no employee_id. Six kinds of mismatch are reported:

    missing_account       active employee with no enabled account
    leaver_still_active   terminated employee with an enabled account (the security risk)
    orphan_account        enabled account that matches no employee
    duplicate_account     active employee with more than one enabled account (all but the oldest listed)
    name_mismatch         account display name differs from the HR name
    department_mismatch   account department differs from HR

Accuracy = share of active employees with exactly one enabled account whose
name and department match HR.
"""
import json

DDL = """
CREATE TABLE IF NOT EXISTS sync.reconciliation_issues (
    run_date date NOT NULL, issue_type text NOT NULL, employee_id text, account_id text, detail text
);
CREATE INDEX IF NOT EXISTS reconciliation_issues_run ON sync.reconciliation_issues (run_date);
CREATE TABLE IF NOT EXISTS sync.reconciliation_summary (
    run_date date PRIMARY KEY, active_employees int, clean_employees int, accuracy numeric(6, 4), issues jsonb
);
"""

ISSUES_SQL = """
WITH emp AS (SELECT * FROM sync.hr_employees),
linked AS (
    SELECT a.account_id, a.display_name, a.department AS account_department, a.upn, a.created_at,
           coalesce(e1.employee_id, e2.employee_id) AS employee_id
    FROM sync.dir_accounts a
    LEFT JOIN emp e1 ON e1.employee_id = a.employee_id
    LEFT JOIN emp e2 ON a.employee_id IS NULL AND lower(e2.email) = lower(a.upn)
    WHERE a.enabled
),
active_links AS (
    SELECT l.*, e.first_name, e.last_name, e.department,
           row_number() OVER (PARTITION BY l.employee_id ORDER BY l.created_at, l.account_id) AS rn
    FROM linked l JOIN emp e USING (employee_id)
    WHERE e.status = 'active'
)
INSERT INTO sync.reconciliation_issues (run_date, issue_type, employee_id, account_id, detail)
SELECT %(run_date)s, 'orphan_account', NULL, account_id, upn FROM linked WHERE employee_id IS NULL
UNION ALL
SELECT %(run_date)s, 'leaver_still_active', l.employee_id, l.account_id, 'terminated ' || e.termination_date
FROM linked l JOIN emp e USING (employee_id) WHERE e.status = 'terminated'
UNION ALL
SELECT %(run_date)s, 'missing_account', e.employee_id, NULL, e.email
FROM emp e WHERE e.status = 'active' AND NOT EXISTS (SELECT 1 FROM linked l WHERE l.employee_id = e.employee_id)
UNION ALL
SELECT %(run_date)s, 'duplicate_account', employee_id, account_id, upn FROM active_links WHERE rn > 1
UNION ALL
SELECT %(run_date)s, 'name_mismatch', employee_id, account_id,
       display_name || ' -> ' || first_name || ' ' || last_name
FROM active_links WHERE rn = 1 AND display_name IS DISTINCT FROM first_name || ' ' || last_name
UNION ALL
SELECT %(run_date)s, 'department_mismatch', employee_id, account_id,
       coalesce(account_department, '<none>') || ' -> ' || coalesce(department, '<none>')
FROM active_links WHERE rn = 1 AND account_department IS DISTINCT FROM department
"""

ISSUE_TYPES = ["missing_account", "leaver_still_active", "orphan_account", "duplicate_account",
               "name_mismatch", "department_mismatch"]
EMPLOYEE_ISSUES = ("missing_account", "duplicate_account", "name_mismatch", "department_mismatch")


def reconcile(conn, run_date) -> dict:
    with conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute("DELETE FROM sync.reconciliation_issues WHERE run_date = %s", (run_date,))
        cur.execute(ISSUES_SQL, {"run_date": run_date})
        cur.execute("SELECT issue_type, count(*) FROM sync.reconciliation_issues WHERE run_date = %s GROUP BY 1",
                    (run_date,))
        counts = {t: 0 for t in ISSUE_TYPES} | dict(cur.fetchall())
        cur.execute("""
            SELECT count(*), count(*) FILTER (WHERE NOT EXISTS (
                SELECT 1 FROM sync.reconciliation_issues i
                WHERE i.run_date = %s AND i.employee_id = e.employee_id AND i.issue_type = ANY(%s)))
            FROM sync.hr_employees e WHERE e.status = 'active'""", (run_date, list(EMPLOYEE_ISSUES)))
        active, clean = cur.fetchone()
        accuracy = round(clean / active, 4) if active else None
        cur.execute("INSERT INTO sync.reconciliation_summary VALUES (%s, %s, %s, %s, %s)"
                    " ON CONFLICT (run_date) DO UPDATE SET active_employees = EXCLUDED.active_employees,"
                    " clean_employees = EXCLUDED.clean_employees, accuracy = EXCLUDED.accuracy, issues = EXCLUDED.issues",
                    (run_date, active, clean, accuracy, json.dumps(counts)))
    conn.commit()
    return {"active_employees": active, "clean_employees": clean, "accuracy": accuracy, "issues": counts}


def issues_for(conn, run_date) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute("SELECT issue_type, employee_id, account_id, detail FROM sync.reconciliation_issues"
                    " WHERE run_date = %s ORDER BY issue_type, employee_id, account_id", (run_date,))
        return [dict(zip(["issue_type", "employee_id", "account_id", "detail"], r)) for r in cur.fetchall()]
