"""Runs the whole pipeline on the synthetic sample data (two weekly batches)."""
import os
import subprocess
import sys

import pytest

from lakehouse.ingest import read_raw
from lakehouse.config import MINES
from lakehouse.pipeline import local_files, run

ROOT = os.path.join(os.path.dirname(__file__), "..")


@pytest.fixture(scope="module")
def sample_dir(tmp_path_factory):
    out = tmp_path_factory.mktemp("sample")
    subprocess.run([sys.executable, os.path.join(ROOT, "scripts", "make_sample_data.py"),
                    "--out", str(out)], check=True, capture_output=True)
    return out


def _load(store, spark, sample_dir, tmp_path, day):
    files = local_files(str(sample_dir / day), str(tmp_path / "landing" / day))
    return run(store, files, day, spark)


def test_two_weekly_loads_are_incremental_and_rerunnable(store, spark, sample_dir, tmp_path):
    week1 = _load(store, spark, sample_dir, tmp_path, "2026-10-30")
    assert week1["silver"]["mines"]["quarantined"] == 1        # the mine with no ID
    assert week1["silver"]["inspections"]["quarantined"] == 6  # 5 end-before-begin + 1 bad date
    assert week1["silver"]["violations"]["quarantined"] == 5   # 4 negative penalties + 1 future date
    assert week1["silver"]["accidents"]["quarantined"] == 2    # missing date + negative days lost

    week2 = _load(store, spark, sample_dir, tmp_path, "2026-11-06")
    assert week2["silver"]["mines"]["inserted"] == 15
    assert week2["silver"]["mines"]["updated"] == 12            # operator changes
    assert week2["silver"]["violations"]["updated"] == 25       # payments recorded (30 minus 5 quarantined)

    rerun = _load(store, spark, sample_dir, tmp_path, "2026-11-06")
    for name, stats in rerun["silver"].items():
        assert stats["inserted"] == 0 and stats["updated"] == 0, name
    assert rerun["bronze"] == {"mines": 0, "inspections": 0, "violations": 0, "accidents": 0}

    for t, key in [("silver_mines", "MINE_ID"), ("silver_violations", "VIOLATION_NO")]:
        df = store.read(t)
        assert df.count() == df.select(key).distinct().count(), t
    dq = store.read("dq_results")
    assert dq.groupBy("dataset", "load_date", "rule").count().where("count > 1").count() == 0


def test_missing_required_column_fails_loudly(spark, tmp_path):
    bad = tmp_path / "Mines.txt"
    bad.write_text("MINE_ID|STATE\n0100003|WV\n", encoding="latin-1")
    with pytest.raises(ValueError, match="missing required columns"):
        read_raw(spark, str(bad), MINES)
