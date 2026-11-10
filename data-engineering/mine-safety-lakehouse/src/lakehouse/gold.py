"""Gold: business-ready tables for analysis and the dashboard.

Gold tables are rebuilt in full from silver on each run. Silver is the
incremental, deduplicated source of truth; gold is small enough that a full
rebuild is simpler and always consistent.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _quarter_cols(date_col: str):
    return [F.year(date_col).alias("year"), F.quarter(date_col).alias("quarter")]


def mine_quarterly_safety(inspections: DataFrame, violations: DataFrame,
                          accidents: DataFrame, mines: DataFrame) -> DataFrame:
    """One row per mine per quarter: inspections, violations, penalties, injuries."""
    k = ["MINE_ID", "year", "quarter"]
    insp = (inspections.select("MINE_ID", *_quarter_cols("INSPECTION_BEGIN_DT"))
            .groupBy(k).agg(F.count(F.lit(1)).alias("inspections")))
    viol = (violations.select("MINE_ID", "SIG_SUB", "PROPOSED_PENALTY", *_quarter_cols("VIOLATION_OCCUR_DT"))
            .groupBy(k).agg(F.count(F.lit(1)).alias("violations"),
                            F.sum(F.when(F.col("SIG_SUB") == "Y", 1).otherwise(0)).alias("ss_violations"),
                            F.coalesce(F.sum("PROPOSED_PENALTY"), F.lit(0)).cast("decimal(14,2)").alias("proposed_penalty")))
    acc = (accidents.select("MINE_ID", "NO_INJURIES", "DAYS_LOST", *_quarter_cols("ACCIDENT_DT"))
           .groupBy(k).agg(F.count(F.lit(1)).alias("accidents"),
                           F.coalesce(F.sum("NO_INJURIES"), F.lit(0)).alias("injuries"),
                           F.coalesce(F.sum("DAYS_LOST"), F.lit(0)).alias("days_lost")))
    out = insp.join(viol, k, "full").join(acc, k, "full").fillna(0, subset=[
        "inspections", "violations", "ss_violations", "accidents", "injuries", "days_lost"])
    out = out.fillna({"proposed_penalty": 0})
    attrs = mines.select("MINE_ID", F.col("CURRENT_MINE_NAME").alias("mine_name"),
                         F.col("STATE").alias("state"), F.col("CURRENT_MINE_TYPE").alias("mine_type"),
                         F.col("COAL_METAL_IND").alias("coal_metal"),
                         F.col("CURRENT_OPERATOR_NAME").alias("operator_name"))
    return (out.join(attrs, "MINE_ID", "left")
            .withColumn("violations_per_inspection",
                        F.round(F.col("violations") / F.when(F.col("inspections") > 0, F.col("inspections")), 2)))


def state_safety_trends(quarterly: DataFrame, mines: DataFrame) -> DataFrame:
    """Yearly totals by state, mine type and coal vs metal/non-metal.

    Injuries per 100 employees uses each mine's CURRENT employee count from the
    Mines file, so it is an approximation for past years (noted in the README).
    """
    yearly = (quarterly.groupBy("state", "mine_type", "coal_metal", "year", "MINE_ID")
              .agg(F.sum("violations").alias("violations"), F.sum("ss_violations").alias("ss_violations"),
                   F.sum("accidents").alias("accidents"), F.sum("injuries").alias("injuries"),
                   F.sum("days_lost").alias("days_lost"), F.sum("proposed_penalty").alias("proposed_penalty")))
    emp = mines.select("MINE_ID", F.coalesce("NO_EMPLOYEES", F.lit(0)).alias("employees"))
    return (yearly.join(emp, "MINE_ID", "left")
            .groupBy("state", "mine_type", "coal_metal", "year")
            .agg(F.countDistinct("MINE_ID").alias("mines_with_activity"),
                 F.sum("violations").alias("violations"), F.sum("ss_violations").alias("ss_violations"),
                 F.sum("accidents").alias("accidents"), F.sum("injuries").alias("injuries"),
                 F.sum("days_lost").alias("days_lost"), F.sum("proposed_penalty").alias("proposed_penalty"),
                 F.sum("employees").alias("employees_current"))
            .withColumn("injuries_per_100_employees_approx",
                        F.round(F.col("injuries") * 100 / F.when(F.col("employees_current") > 0,
                                                                  F.col("employees_current")), 2)))


def repeat_violators(quarterly: DataFrame, window_quarters: int = 4, min_quarters: int = 3) -> DataFrame:
    """Mines with significant-and-substantial (S&S) violations in at least
    `min_quarters` of the latest `window_quarters` quarters in the data."""
    q = quarterly.withColumn("q_index", F.col("year") * 4 + F.col("quarter") - 1)
    latest = q.agg(F.max("q_index")).first()[0]
    if latest is None:
        return q.limit(0)
    recent = q.where(F.col("q_index") > latest - window_quarters)
    return (recent.groupBy("MINE_ID", "mine_name", "state", "mine_type", "operator_name")
            .agg(F.sum(F.when(F.col("ss_violations") > 0, 1).otherwise(0)).alias("quarters_with_ss"),
                 F.sum("ss_violations").alias("ss_violations"),
                 F.sum("violations").alias("violations"),
                 F.sum("proposed_penalty").alias("proposed_penalty"),
                 F.sum("injuries").alias("injuries"))
            .where(F.col("quarters_with_ss") >= min_quarters)
            .withColumn("rank", F.dense_rank().over(Window.orderBy(F.col("ss_violations").desc())))
            .orderBy("rank"))


def equipment_accidents(accidents: DataFrame) -> DataFrame:
    """Accidents by equipment and accident type, with the share that were
    serious (fatal or permanent disability)."""
    serious = F.upper(F.col("DEGREE_INJURY")).rlike("FATAL|PERM")
    return (accidents
            .withColumn("year", F.year("ACCIDENT_DT"))
            .withColumn("equipment", F.coalesce("MINING_EQUIP", F.lit("NOT REPORTED")))
            .withColumn("accident_type", F.coalesce("ACCIDENT_TYPE", F.lit("NOT REPORTED")))
            .groupBy("equipment", "accident_type", "year")
            .agg(F.count(F.lit(1)).alias("accidents"),
                 F.coalesce(F.sum("NO_INJURIES"), F.lit(0)).alias("injuries"),
                 F.coalesce(F.sum("DAYS_LOST"), F.lit(0)).alias("days_lost"),
                 F.sum(F.when(serious, 1).otherwise(0)).alias("serious_accidents"))
            .withColumn("serious_pct", F.round(F.col("serious_accidents") * 100 / F.col("accidents"), 1)))


def build_gold(store) -> dict:
    mines = store.read("silver_mines")
    inspections = store.read("silver_inspections")
    violations = store.read("silver_violations")
    accidents = store.read("silver_accidents")

    quarterly = mine_quarterly_safety(inspections, violations, accidents, mines)
    store.overwrite(quarterly, "gold_mine_quarterly_safety")
    quarterly = store.read("gold_mine_quarterly_safety")  # reuse the written table

    tables = {
        "gold_state_safety_trends": state_safety_trends(quarterly, mines),
        "gold_repeat_violators": repeat_violators(quarterly),
        "gold_equipment_accidents": equipment_accidents(accidents),
    }
    counts = {"gold_mine_quarterly_safety": quarterly.count()}
    for name, df in tables.items():
        store.overwrite(df, name)
        counts[name] = store.read(name).count()
    print(f"[gold] {counts}")
    return counts
