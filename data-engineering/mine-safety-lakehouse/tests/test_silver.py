from pyspark.sql import functions as F

from lakehouse.config import MINES, VIOLATIONS
from lakehouse.quality import RULES
from lakehouse.silver import add_row_hash, apply_rules, deduplicate, standardize


def _strings(spark, rows, cols):
    return spark.createDataFrame(rows, ", ".join(f"{c} string" for c in cols))


def test_standardize_trims_blanks_and_restores_leading_zeros(spark):
    df = _strings(spark, [("  4608  ", " Big Mine ", "", "12"), ("0100003", "Other", "WV", "abc")],
                  ["MINE_ID", "CURRENT_MINE_NAME", "STATE", "NO_EMPLOYEES"])
    out, cast_rules = standardize(df, MINES)
    rows = {r.MINE_ID: r for r in out.collect()}

    assert set(rows) == {"0004608", "0100003"}
    assert rows["0004608"].CURRENT_MINE_NAME == "Big Mine"
    assert rows["0004608"].STATE is None            # blank became NULL
    assert rows["0004608"].NO_EMPLOYEES == 12
    assert rows["0100003"].NO_EMPLOYEES is None      # "abc" could not be cast...
    bad = out.where(F.expr(cast_rules[0].fails_when)).count()
    assert bad == 1                                  # ...and is counted, not silently lost


def test_dates_parse_in_both_formats_and_bad_dates_become_null(spark):
    df = _strings(spark, [("1", "01/15/2024"), ("2", "2024-02-20"), ("3", "2024-13-45")],
                  ["VIOLATION_NO", "VIOLATION_OCCUR_DT"])
    out, _ = standardize(df, VIOLATIONS)
    got = {r.VIOLATION_NO: r.VIOLATION_OCCUR_DT for r in out.collect()}
    assert str(got["1"]) == "2024-01-15"
    assert str(got["2"]) == "2024-02-20"
    assert got["3"] is None


def test_deduplicate_is_deterministic(spark):
    df = _strings(spark, [("A", "x"), ("A", "x"), ("A", "y"), ("B", "z")], ["K", "V"])
    df = add_row_hash(df, ["K", "V"])
    first, dropped = deduplicate(df, ["K"])
    second, _ = deduplicate(df.orderBy(F.rand()), ["K"])
    assert dropped == 2
    assert sorted(first.collect()) == sorted(second.collect())


def test_violation_rules_flag_errors_and_warnings(spark):
    rows = [
        ("1", "0100003", "Y", 100.0, "2024-01-01"),
        ("2", None, "N", 50.0, "2024-01-01"),        # missing mine -> error
        ("3", "0100003", "N", -5.0, "2024-01-01"),   # negative penalty -> error
        ("4", "0100003", "?", 10.0, "2024-01-01"),   # odd S&S flag -> warning only
    ]
    df = spark.createDataFrame(rows, "VIOLATION_NO string, MINE_ID string, SIG_SUB string, "
                                     "PROPOSED_PENALTY double, VIOLATION_OCCUR_DT string")
    df = df.withColumn("VIOLATION_OCCUR_DT", F.to_date("VIOLATION_OCCUR_DT"))
    out = {r.VIOLATION_NO: r for r in apply_rules(df, RULES["violations"]).collect()}

    assert out["1"]._dq_errors == [] and out["1"]._dq_warnings == []
    assert out["2"]._dq_errors == ["mine_id_missing"]
    assert out["3"]._dq_errors == ["penalty_negative"]
    assert out["4"]._dq_errors == [] and out["4"]._dq_warnings == ["sig_sub_not_y_n"]
