from pyspark.sql import functions as F

from lakehouse.gold import repeat_violators
from lakehouse.silver import add_row_hash


def _batch(spark, rows):
    df = spark.createDataFrame(rows, "ID string, NAME string")
    return add_row_hash(df, ["ID", "NAME"])


def test_upsert_counts_and_never_duplicates(store, spark):
    first = store.upsert(_batch(spark, [("1", "a"), ("2", "b")]), "t", ["ID"])
    assert (first.inserted, first.updated, first.unchanged) == (2, 0, 0)

    again = store.upsert(_batch(spark, [("1", "a"), ("2", "b")]), "t", ["ID"])
    assert (again.inserted, again.updated, again.unchanged) == (0, 0, 2)

    changed = store.upsert(_batch(spark, [("1", "a"), ("2", "B"), ("3", "c")]), "t", ["ID"])
    assert (changed.inserted, changed.updated, changed.unchanged) == (1, 1, 1)

    final = {r.ID: r.NAME for r in store.read("t").collect()}
    assert final == {"1": "a", "2": "B", "3": "c"}


def test_upsert_keeps_rows_missing_from_a_later_batch(store, spark):
    store.upsert(_batch(spark, [("1", "a"), ("2", "b")]), "t", ["ID"])
    store.upsert(_batch(spark, [("2", "b")]), "t", ["ID"])
    assert store.read("t").count() == 2


def test_repeat_violators_needs_three_of_last_four_quarters(spark):
    rows = []
    # Mine A: S&S violations in 3 of the last 4 quarters -> flagged
    for y, q, ss in [(2025, 4, 2), (2026, 1, 1), (2026, 2, 0), (2026, 3, 4)]:
        rows.append(("A", y, q, ss, ss + 1))
    # Mine B: S&S only in 2 recent quarters, plus an old one outside the window
    for y, q, ss in [(2024, 1, 9), (2026, 2, 1), (2026, 3, 1)]:
        rows.append(("B", y, q, ss, ss))
    df = (spark.createDataFrame(rows, "MINE_ID string, year int, quarter int, ss_violations long, violations long")
          .withColumn("mine_name", F.col("MINE_ID")).withColumn("state", F.lit("WV"))
          .withColumn("mine_type", F.lit("Surface")).withColumn("operator_name", F.lit("Op"))
          .withColumn("proposed_penalty", F.lit(0)).withColumn("injuries", F.lit(0)))
    out = repeat_violators(df).collect()
    assert [r.MINE_ID for r in out] == ["A"]
    assert out[0].quarters_with_ss == 3
    assert out[0].ss_violations == 7
