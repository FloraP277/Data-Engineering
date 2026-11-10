"""Simulate N days: HR changes -> daily run -> IT fixes part of the report.

    DIRSYNC_DSN=postgresql://... python -m dirsync.demo --employees 6000 --days 30

Writes reports/daily.csv and reports/accuracy.png. WARNING: this resets the
hr, directory and sync schemas in the target database.
"""
import argparse
import datetime as dt
import os

import pandas as pd
import psycopg

from .reconcile import ISSUE_TYPES, issues_for
from .run import run
from .simulate import Simulator


def reset(conn):
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS sync CASCADE; DROP SCHEMA IF EXISTS hr CASCADE;"
                    " DROP SCHEMA IF EXISTS directory CASCADE;")
    conn.commit()


def simulate_days(conn, employees: int, days: int, start: dt.date, fix_rate: float, seed: int = 1) -> pd.DataFrame:
    reset(conn)
    sim = Simulator(conn, seed=seed)
    sim.setup(employees, start - dt.timedelta(days=1))
    run(conn, start - dt.timedelta(days=1))            # initial full load
    rows = []
    for i in range(days):
        day = start + dt.timedelta(days=i)
        changes = sim.advance(day)
        res = run(conn, day)
        issues = issues_for(conn, day)
        fixed = sim.apply_fixes(issues, day, fix_rate)
        rec = res["reconciliation"]
        rows.append({"date": day, "hr_changes": changes["hr"], "directory_changes": changes["directory"],
                     "hr_synced": res["sync"]["hr"]["inserted"] + res["sync"]["hr"]["updated"],
                     "directory_synced": res["sync"]["directory"]["inserted"] + res["sync"]["directory"]["updated"],
                     "active_employees": rec["active_employees"], "accuracy": rec["accuracy"],
                     **{t: rec["issues"][t] for t in ISSUE_TYPES}, "fixed_by_it": fixed})
    return pd.DataFrame(rows)


def plot(df: pd.DataFrame, path: str):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    # Both panels use positions 0..n-1 so the line and the bars line up.
    x = range(len(df))
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(9, 6.5), sharex=True, height_ratios=[1, 1.4])
    ax1.plot(x, df["accuracy"] * 100, marker="o", markersize=3)
    ax1.set_ylabel("Accuracy (%)")
    ax1.set_title("Directory accuracy after each daily reconciliation")
    ax1.grid(axis="y", alpha=0.3)
    bottom = [0] * len(df)
    for t in ISSUE_TYPES:
        ax2.bar(x, df[t], bottom=bottom, label=t.replace("_", " "), width=0.8)
        bottom = [b + v for b, v in zip(bottom, df[t])]
    ax2.set_ylabel("Open issues")
    ax2.set_xticks(list(x)[::2], [pd.Timestamp(d).strftime("%b %d") for d in df["date"]][::2], rotation=45)
    ax2.legend(ncol=3, fontsize=8, loc="upper center", bbox_to_anchor=(0.5, -0.28), frameon=False)
    fig.tight_layout()
    fig.savefig(path, dpi=130)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dsn", default=os.environ.get("DIRSYNC_DSN"))
    p.add_argument("--employees", type=int, default=6000)
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--start", default="2026-10-01")
    p.add_argument("--fix-rate", type=float, default=0.3, help="Share of reported issues IT fixes each day")
    p.add_argument("--out", default="reports")
    args = p.parse_args(argv)
    if not args.dsn:
        p.error("set DIRSYNC_DSN or pass --dsn")
    with psycopg.connect(args.dsn) as conn:
        df = simulate_days(conn, args.employees, args.days, dt.date.fromisoformat(args.start), args.fix_rate)
    os.makedirs(args.out, exist_ok=True)
    df.to_csv(os.path.join(args.out, "daily.csv"), index=False)
    plot(df, os.path.join(args.out, "accuracy.png"))
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
