"""Data quality checks on the synced copies, run before reconciliation.

Each check is a SQL query returning the number of failing rows. A failing
`error` check stops the run, because reconciling bad data would send IT a wrong
worklist. `warn` checks are recorded and the run continues.
"""

CHECKS = [
    ("hr_active_missing_email", "error",
     "SELECT count(*) FROM sync.hr_employees WHERE status = 'active' AND coalesce(email, '') = ''"),
    ("hr_terminated_without_date", "error",
     "SELECT count(*) FROM sync.hr_employees WHERE status = 'terminated' AND termination_date IS NULL"),
    ("hr_active_duplicate_email", "error",
     "SELECT count(*) FROM (SELECT lower(email) FROM sync.hr_employees WHERE status = 'active'"
     " GROUP BY 1 HAVING count(*) > 1) d"),
    ("dir_enabled_missing_upn", "error",
     "SELECT count(*) FROM sync.dir_accounts WHERE enabled AND coalesce(upn, '') = ''"),
    ("hr_complete", "error",
     "SELECT abs((SELECT count(*) FROM hr.employees) - (SELECT count(*) FROM sync.hr_employees))"),
    ("dir_complete", "error",
     "SELECT abs((SELECT count(*) FROM directory.accounts) - (SELECT count(*) FROM sync.dir_accounts))"),
    ("hr_hire_date_in_future", "warn",
     "SELECT count(*) FROM sync.hr_employees WHERE hire_date > %(run_date)s"),
    ("dir_account_without_employee_id", "warn",
     "SELECT count(*) FROM sync.dir_accounts WHERE enabled AND employee_id IS NULL"),
]

DDL = """
CREATE TABLE IF NOT EXISTS sync.check_results (
    run_date date, check_name text, severity text, failed bigint, PRIMARY KEY (run_date, check_name)
);
"""


class ChecksFailed(RuntimeError):
    pass


def run_checks(conn, run_date) -> list[dict]:
    results = []
    with conn.cursor() as cur:
        cur.execute(DDL)
        for name, severity, sql in CHECKS:
            cur.execute(sql, {"run_date": run_date})
            failed = cur.fetchone()[0]
            results.append({"check": name, "severity": severity, "failed": failed})
            cur.execute("INSERT INTO sync.check_results VALUES (%s, %s, %s, %s)"
                        " ON CONFLICT (run_date, check_name) DO UPDATE SET failed = EXCLUDED.failed",
                        (run_date, name, severity, failed))
    conn.commit()
    return results
