# Databricks notebook source
# MAGIC %md
# MAGIC # Mine Safety Lakehouse: weekly run
# MAGIC
# MAGIC Downloads the latest MSHA files into a Unity Catalog volume, then builds
# MAGIC bronze, silver and gold Delta tables in `<catalog>.<schema>`.
# MAGIC
# MAGIC Run it by hand the first time. After that, the weekly job in `databricks.yml`
# MAGIC runs it every Saturday morning, after MSHA's Friday afternoon update.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "mine_safety")
dbutils.widgets.text("load_date", "")   # blank = today

# COMMAND ----------

import datetime as dt
import os
import sys

# The repo's src/ folder sits next to notebooks/ in the Git folder.
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from lakehouse.pipeline import main

load_date = dbutils.widgets.get("load_date") or dt.date.today().isoformat()
summary = main([
    "--mode", "databricks",
    "--catalog", dbutils.widgets.get("catalog"),
    "--schema", dbutils.widgets.get("schema"),
    "--load-date", load_date,
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the run

# COMMAND ----------

catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
display(spark.sql(f"""
    SELECT dataset, rule, severity, failed, total, failed_pct
    FROM {catalog}.{schema}.dq_results
    WHERE load_date = '{load_date}' AND failed > 0
    ORDER BY severity, failed DESC
"""))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalog}.{schema}.gold_repeat_violators ORDER BY rank LIMIT 20"))
