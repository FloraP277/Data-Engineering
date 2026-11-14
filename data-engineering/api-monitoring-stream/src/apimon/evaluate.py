"""Score alerts against the injected incidents (ground truth).

An incident counts as detected when an alert fires for an affected service in
a window that overlaps the incident. Detection delay is measured from the
incident start to the END of the first alerting window (the earliest moment
the window could be complete). Alerts that overlap no incident are false alarms.

    python -m apimon.evaluate --out data/out --incidents data/incidents.json
"""
import argparse
import json
import os
from datetime import datetime, timedelta

from .simulator import SERVICES


def _t(s):
    return datetime.fromisoformat(s)


def score(alerts: list[dict], incidents: list[dict], grace_minutes: int = 2) -> dict:
    window = timedelta(minutes=1)
    results, used = [], set()
    for inc in incidents:
        start, end = _t(inc["start"]), _t(inc["end"]) + timedelta(minutes=grace_minutes)
        affected = [inc["service"]] if inc["service"] else list(SERVICES)   # a region outage hits every service
        hits = sorted((a for a in alerts if a["service"] in affected
                       and _t(a["window_start"]) + window > start and _t(a["window_start"]) < end),
                      key=lambda a: a["window_start"])
        used |= {(a["service"], a["window_start"], a["rule"]) for a in hits}
        delay = ((_t(hits[0]["window_start"]) + window - start).total_seconds() if hits else None)
        results.append({"kind": inc["kind"], "service": inc["service"] or f"all ({inc['region']})",
                        "detected": bool(hits), "delay_seconds": delay,
                        "rules": sorted({a["rule"] for a in hits})})
    false_alarms = [a for a in alerts if (a["service"], a["window_start"], a["rule"]) not in used]
    detected = sum(r["detected"] for r in results)
    return {"incidents": results, "detected": detected, "total_incidents": len(results),
            "false_alarms": len(false_alarms), "total_alerts": len(alerts),
            "precision": round((len(alerts) - len(false_alarms)) / len(alerts), 3) if alerts else None,
            "recall": round(detected / len(results), 3) if results else None}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", default="data/out")
    p.add_argument("--incidents", default="data/incidents.json")
    args = p.parse_args(argv)
    alerts = [json.loads(l) for l in open(os.path.join(args.out, "alerts.jsonl"))]
    report = score(alerts, json.load(open(args.incidents)))
    print(json.dumps(report, indent=2))
    return report


if __name__ == "__main__":
    main()
