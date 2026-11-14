import json
import os
import sys
from datetime import datetime, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from apimon import alerts as alert_rules     # noqa: E402
from apimon.evaluate import score            # noqa: E402
from apimon.simulator import generate, to_files  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    from apimon.streaming import get_spark
    s = get_spark()
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def test_simulator_injects_incidents_duplicates_and_late_events():
    batches = list(generate(minutes=8, rate=5, seed=3))
    incidents = batches[0][2]
    assert [i.kind for i in incidents] == ["latency_spike", "error_burst", "region_outage"]
    events = [e for _, evs, _ in batches for e in evs]
    ids = [e["event_id"] for e in events]
    assert len(ids) > len(set(ids))                                   # duplicates delivered
    late = sum(1 for m, evs, _ in batches for e in evs
               if datetime.fromisoformat(e["event_time"]).minute < 0 + m)   # event from an earlier minute
    assert late > 0


def test_windowed_metrics_on_static_data(spark):
    from apimon.streaming import windowed
    rows = [("a", "2026-11-06T12:00:10", "pay", 200, 100.0), ("b", "2026-11-06T12:00:20", "pay", 500, 300.0),
            ("c", "2026-11-06T12:00:30", "pay", 200, 200.0), ("d", "2026-11-06T12:01:05", "pay", 200, 50.0)]
    df = spark.createDataFrame(rows, "event_id string, t string, service string, status int, latency_ms double")
    out = windowed(df.withColumn("event_ts", df["t"].cast("timestamp"))).orderBy("window_start").collect()
    assert [r.requests for r in out] == [3, 1]
    assert out[0].errors == 1 and out[0].error_rate == pytest.approx(1 / 3)
    assert out[0].p95_latency_ms == 300.0


def _win(minute, p95=100.0, err=0.0, n=100, svc="payments"):
    return {"window_start": f"2026-11-06T12:{minute:02d}:00+00:00", "service": svc, "requests": n,
            "error_rate": err, "p95_latency_ms": p95}


def test_alert_rules():
    history = {}
    normal = [_win(m) for m in range(6)]
    assert alert_rules.evaluate_windows(normal, history) == []
    spike = alert_rules.evaluate_windows([_win(6, p95=500.0)], history)
    assert {a["rule"] for a in spike} == {"p95_above_baseline"}           # under the fixed limit, over baseline
    burst = alert_rules.evaluate_windows([_win(7, err=0.2)], history)
    assert {a["rule"] for a in burst} == {"error_rate_above_limit", "error_rate_above_baseline"}
    assert alert_rules.evaluate_windows([_win(8, err=0.5, n=4)], history) == []   # too little traffic


def test_score_counts_detections_delay_and_false_alarms():
    incidents = [{"kind": "error_burst", "start": "2026-11-06T12:10:00+00:00", "end": "2026-11-06T12:14:00+00:00",
                  "service": "checkout", "region": None}]
    alerts = [{"window_start": "2026-11-06T12:11:00+00:00", "service": "checkout", "rule": "r", "value": 1, "threshold": 0},
              {"window_start": "2026-11-06T12:30:00+00:00", "service": "checkout", "rule": "r", "value": 1, "threshold": 0}]
    s = score(alerts, incidents)
    assert s["detected"] == 1 and s["false_alarms"] == 1 and s["precision"] == 0.5
    assert s["incidents"][0]["delay_seconds"] == 120.0


def test_streaming_end_to_end_detects_all_incidents(spark, tmp_path):
    from apimon.streaming import run
    incidents = to_files(str(tmp_path / "events"), minutes=20, rate=10, seed=1)
    stats = run(spark, "files", str(tmp_path / "out"), path=str(tmp_path / "events"), once=True)
    assert stats["input_rows"] > 0

    events = [json.loads(l) for f in sorted((tmp_path / "events").glob("batch-*.json")) for l in open(f)]
    windows = [json.loads(l) for l in open(tmp_path / "out" / "windows.jsonl")]
    assert sum(w["requests"] for w in windows) == len({e["event_id"] for e in events})   # duplicates dropped

    alerts = [json.loads(l) for l in open(tmp_path / "out" / "alerts.jsonl")]
    report = score(alerts, json.load(open(tmp_path / "incidents.json")))
    assert report["recall"] == 1.0
    assert report["precision"] >= 0.9

    again = run(spark, "files", str(tmp_path / "out"), path=str(tmp_path / "events"), once=True)
    assert again["input_rows"] == 0                                   # checkpoint: nothing re-read
