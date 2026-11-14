"""Alert rules applied to each window.

1. Static thresholds: error rate above 5%, or p95 latency above a per-service limit.
2. Baseline: p95 latency or error rate far above the service's own recent normal
   (median of its last 15 windows), which adapts to each service without hand-tuning.

Windows with very little traffic are skipped: a 1-in-3 error rate on three
requests is noise, not an incident.
"""
from statistics import median

P95_LIMIT_MS = {"auth": 200, "catalog": 400, "cart": 300, "checkout": 600, "payments": 900}
ERROR_RATE_LIMIT = 0.05
BASELINE_FACTOR = 3.0
BASELINE_WINDOWS = 15


def evaluate_windows(rows: list[dict], history: dict, min_requests: int = 20) -> list[dict]:
    alerts = []
    for r in sorted(rows, key=lambda x: x["window_start"]):
        if r["requests"] < min_requests:
            continue
        svc, past = r["service"], history.setdefault(r["service"], {})
        base = [w for ws, w in sorted(past.items()) if ws < r["window_start"]][-BASELINE_WINDOWS:]

        def alert(rule, value, threshold):
            alerts.append({"window_start": r["window_start"], "service": svc, "rule": rule,
                           "value": round(float(value), 4), "threshold": round(float(threshold), 4)})

        if r["error_rate"] > ERROR_RATE_LIMIT:
            alert("error_rate_above_limit", r["error_rate"], ERROR_RATE_LIMIT)
        limit = P95_LIMIT_MS.get(svc)
        if limit and r["p95_latency_ms"] > limit:
            alert("p95_above_limit", r["p95_latency_ms"], limit)
        if len(base) >= 5:
            p95_base = median(w["p95_latency_ms"] for w in base)
            if r["p95_latency_ms"] > BASELINE_FACTOR * p95_base:
                alert("p95_above_baseline", r["p95_latency_ms"], BASELINE_FACTOR * p95_base)
            err_base = max(median(w["error_rate"] for w in base), 0.005)
            if r["error_rate"] > BASELINE_FACTOR * err_base and r["error_rate"] > 0.02:
                alert("error_rate_above_baseline", r["error_rate"], BASELINE_FACTOR * err_base)
        # Keep the latest version of each window as history (updates replace earlier partial counts).
        past[r["window_start"]] = r
    return alerts
