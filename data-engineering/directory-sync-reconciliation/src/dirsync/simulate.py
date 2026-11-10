"""Simulated HR system and identity directory that slowly drift apart.

Every simulated day, HR records hires, leavers, name changes and department
transfers. The directory is supposed to follow, but sometimes doesn't (the
"lag" probabilities), and now and then someone creates a duplicate account or a
contractor account with no employee behind it. These are the problems the
reconciliation must find. All names and emails are fake (Faker, example.com).
"""
import random
from datetime import date, datetime, time, timedelta, timezone

from faker import Faker

DEPARTMENTS = ["Engineering", "Finance", "Human Resources", "Operations", "Sales", "Security",
               "Mining Operations", "Procurement", "Legal", "IT"]
TITLES = ["Analyst", "Engineer", "Manager", "Specialist", "Technician", "Coordinator", "Director"]

EMP_COLS = ["employee_id", "first_name", "last_name", "email", "department", "title", "status",
            "hire_date", "termination_date", "updated_at"]
ACCT_COLS = ["account_id", "employee_id", "upn", "display_name", "department", "enabled",
             "created_at", "updated_at"]

SOURCE_DDL = """
CREATE SCHEMA IF NOT EXISTS hr;
CREATE SCHEMA IF NOT EXISTS directory;
CREATE TABLE IF NOT EXISTS hr.employees (
    employee_id      text PRIMARY KEY,
    first_name       text NOT NULL,
    last_name        text NOT NULL,
    email            text,
    department       text,
    title            text,
    status           text NOT NULL CHECK (status IN ('active', 'terminated')),
    hire_date        date,
    termination_date date,
    updated_at       timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS directory.accounts (
    account_id   text PRIMARY KEY,
    employee_id  text,
    upn          text,
    display_name text,
    department   text,
    enabled      boolean NOT NULL,
    created_at   timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL
);
"""

# Probability that the directory is NOT updated after an HR event.
DEFAULT_LAG = {"hire": 0.10, "termination": 0.15, "name": 0.30, "transfer": 0.20}


class Simulator:
    def __init__(self, conn, seed: int = 1, lag: dict | None = None, daily_rate: float = 0.002):
        self.conn, self.rng = conn, random.Random(seed)
        self.fake = Faker()
        self.fake.seed_instance(seed)
        self.lag = {**DEFAULT_LAG, **(lag or {})}
        self.rate = daily_rate
        self.emps, self.accts = {}, {}
        self.next_emp, self.next_acct, self.next_contractor = 100000, 1, 1

    # ---- helpers ---------------------------------------------------------------
    def _ts(self, day: date, lo: int = 8, hi: int = 17) -> datetime:
        return datetime.combine(day, time(self.rng.randint(lo, hi - 1), self.rng.randint(0, 59)), tzinfo=timezone.utc)

    def _new_emp(self, day: date, ts: datetime) -> dict:
        eid = f"E{self.next_emp}"
        self.next_emp += 1
        first, last = self.fake.first_name(), self.fake.last_name()
        e = {"employee_id": eid, "first_name": first, "last_name": last,
             "email": f"{first}.{last}.{eid[1:]}@example.com".lower().replace("'", ""),
             "department": self.rng.choice(DEPARTMENTS), "title": self.rng.choice(TITLES), "status": "active",
             "hire_date": day, "termination_date": None, "updated_at": ts}
        self.emps[eid] = e
        return e

    def _new_acct(self, employee_id, upn, display_name, department, ts) -> dict:
        aid = f"A{self.next_acct:07d}"
        self.next_acct += 1
        a = {"account_id": aid, "employee_id": employee_id, "upn": upn, "display_name": display_name,
             "department": department, "enabled": True, "created_at": ts, "updated_at": ts}
        self.accts[aid] = a
        return a

    def _acct_for(self, e: dict, ts: datetime) -> dict:
        return self._new_acct(e["employee_id"], e["email"], f"{e['first_name']} {e['last_name']}", e["department"], ts)

    def _enabled_accounts(self, eid: str) -> list[dict]:
        return [a for a in self.accts.values() if a["employee_id"] == eid and a["enabled"]]

    def _write(self, emp_ids, acct_ids):
        with self.conn.cursor() as cur:
            if emp_ids:
                cur.executemany(
                    f"INSERT INTO hr.employees ({', '.join(EMP_COLS)}) VALUES ({', '.join(['%s'] * len(EMP_COLS))})"
                    f" ON CONFLICT (employee_id) DO UPDATE SET "
                    + ", ".join(f"{c} = EXCLUDED.{c}" for c in EMP_COLS[1:]),
                    [[self.emps[i][c] for c in EMP_COLS] for i in emp_ids])
            if acct_ids:
                cur.executemany(
                    f"INSERT INTO directory.accounts ({', '.join(ACCT_COLS)}) VALUES ({', '.join(['%s'] * len(ACCT_COLS))})"
                    f" ON CONFLICT (account_id) DO UPDATE SET "
                    + ", ".join(f"{c} = EXCLUDED.{c}" for c in ACCT_COLS[1:]),
                    [[self.accts[i][c] for c in ACCT_COLS] for i in acct_ids])
        self.conn.commit()

    # ---- public ------------------------------------------------------------------
    def setup(self, employees: int, day: date):
        """A clean starting point: every employee has exactly one matching account."""
        with self.conn.cursor() as cur:
            cur.execute(SOURCE_DDL)
            cur.execute("TRUNCATE hr.employees, directory.accounts")
        self.conn.commit()
        for _ in range(employees):
            ts = self._ts(day)
            e = self._new_emp(day - timedelta(days=self.rng.randint(30, 4000)), ts)
            self._acct_for(e, ts)
        self._write(list(self.emps), list(self.accts))

    def advance(self, day: date) -> dict:
        """One day of HR changes, with the directory sometimes lagging. Returns how
        many HR and directory records changed."""
        rng, ch_e, ch_a = self.rng, set(), set()
        active = [e for e in self.emps.values() if e["status"] == "active"]
        n = lambda k=1.0: max(0, round(len(active) * self.rate * k * rng.uniform(0.5, 1.5)))  # noqa: E731

        for _ in range(n()):                                            # hires
            ts = self._ts(day)
            e = self._new_emp(day, ts)
            ch_e.add(e["employee_id"])
            if rng.random() > self.lag["hire"]:
                ch_a.add(self._acct_for(e, ts)["account_id"])
        for e in rng.sample(active, min(n(), len(active))):              # leavers
            ts = self._ts(day)
            e.update(status="terminated", termination_date=day, updated_at=ts)
            ch_e.add(e["employee_id"])
            if rng.random() > self.lag["termination"]:
                for a in self._enabled_accounts(e["employee_id"]):
                    a.update(enabled=False, updated_at=ts)
                    ch_a.add(a["account_id"])
        still = [e for e in self.emps.values() if e["status"] == "active"]
        for e in rng.sample(still, min(n(0.25), len(still))):            # name changes
            ts = self._ts(day)
            e.update(last_name=self.fake.last_name(), updated_at=ts)
            ch_e.add(e["employee_id"])
            if rng.random() > self.lag["name"]:
                for a in self._enabled_accounts(e["employee_id"]):
                    a.update(display_name=f"{e['first_name']} {e['last_name']}", updated_at=ts)
                    ch_a.add(a["account_id"])
        for e in rng.sample(still, min(n(1.5), len(still))):             # transfers
            ts = self._ts(day)
            e.update(department=rng.choice([d for d in DEPARTMENTS if d != e["department"]]), updated_at=ts)
            ch_e.add(e["employee_id"])
            if rng.random() > self.lag["transfer"]:
                for a in self._enabled_accounts(e["employee_id"]):
                    a.update(department=e["department"], updated_at=ts)
                    ch_a.add(a["account_id"])
        if rng.random() < 0.3:                                          # someone creates a second account
            e = rng.choice(still)
            ts = self._ts(day)
            a = self._new_acct(e["employee_id"], e["email"].replace("@", ".2@"),
                               f"{e['first_name']} {e['last_name']}", e["department"], ts)
            ch_a.add(a["account_id"])
        if rng.random() < 0.5:                                          # contractor account, no HR record
            ts = self._ts(day)
            a = self._new_acct(None, f"contractor{self.next_contractor}@example.com",
                               self.fake.name(), "Contractors", ts)
            self.next_contractor += 1
            ch_a.add(a["account_id"])
        self._write(ch_e, ch_a)
        return {"hr": len(ch_e), "directory": len(ch_a)}

    def apply_fixes(self, issues: list[dict], day: date, fix_rate: float) -> int:
        """IT works through part of the reconciliation report after the run."""
        fixed, ch_a = 0, set()
        for issue in issues:
            if self.rng.random() > fix_rate:
                continue
            ts, kind = self._ts(day, 18, 23), issue["issue_type"]
            e = self.emps.get(issue["employee_id"]) if issue["employee_id"] else None
            a = self.accts.get(issue["account_id"]) if issue["account_id"] else None
            if kind == "missing_account" and e:
                a = self._acct_for(e, ts)
            elif kind in ("leaver_still_active", "orphan_account", "duplicate_account") and a:
                a.update(enabled=False, updated_at=ts)
            elif kind == "name_mismatch" and a and e:
                a.update(display_name=f"{e['first_name']} {e['last_name']}", updated_at=ts)
            elif kind == "department_mismatch" and a and e:
                a.update(department=e["department"], updated_at=ts)
            else:
                continue
            ch_a.add(a["account_id"])
            fixed += 1
        self._write(set(), ch_a)
        return fixed
