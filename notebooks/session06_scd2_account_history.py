# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.gold;

# COMMAND ----------

from typing import TYPE_CHECKING
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Type hint for local IDEs/linters (VS Code); Databricks injects 'spark' at runtime
if TYPE_CHECKING:
    spark: SparkSession = None  # type: ignore

raw_schema = StructType([
    StructField("account_id", StringType(), False),
    StructField("account_status", StringType(), False),
    StructField("credit_limit", IntegerType(), False),
    StructField("effective_date", StringType(), False),
])

# 1. Load initial account dimension state
initial = spark.read.option("header", True).schema(raw_schema).csv(
    "/Volumes/workspace/landing/raw_files/account_snapshots_initial.csv"
)

initial_versioned = (
    initial
    .withColumnRenamed("effective_date", "valid_from")
    .withColumn("valid_to", lit("9999-12-31"))
    .withColumn("is_current", lit(True))
)

initial_versioned.write.format("delta").mode("overwrite").saveAsTable("workspace.gold.account_history")
print(f"initial load: {initial_versioned.count()} rows")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.gold.account_history WHERE account_id = 'ACC00002' ORDER BY valid_from;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM workspace.gold.account_history VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.gold.account_history WHERE account_id = 'ACC00125' ORDER BY valid_from;

# COMMAND ----------

from delta.tables import DeltaTable

# 2. Parameterize batch file input using Databricks Widgets
dbutils.widgets.text("batch_file", "account_snapshots_mixed_batch.csv")
batch_file = dbutils.widgets.get("batch_file")

raw_schema = StructType([
    StructField("account_id", StringType(), False),
    StructField("account_status", StringType(), False),
    StructField("credit_limit", IntegerType(), False),
    StructField("effective_date", StringType(), False),
])

target = DeltaTable.forName(spark, "workspace.gold.account_history")
account_history = target.toDF()
current = account_history.filter(col("is_current") == True)

batch = spark.read.option("header", True).schema(raw_schema).csv(
    f"/Volumes/workspace/landing/raw_files/{batch_file}"
)

joined = batch.join(
    current.select(
        col("account_id").alias("cur_account_id"),
        col("valid_from").alias("current_valid_from"),
        col("account_status").alias("current_status"),
        col("credit_limit").alias("current_credit_limit"),
    ),
    batch.account_id == col("cur_account_id"),
)

# Normal updates: on-time or forward, with genuine attribute changes from the current active row
normal = joined.where(
    (col("effective_date") >= col("current_valid_from")) &
    ((col("account_status") != col("current_status")) | (col("credit_limit") != col("current_credit_limit")))
)

normal_shrink = normal.select(
    "account_id",
    col("current_valid_from").alias("valid_from"),
    lit(None).cast("string").alias("account_status"),
    lit(None).cast("int").alias("credit_limit"),
    col("effective_date").alias("valid_to"),
)

normal_insert = normal.select(
    "account_id",
    col("effective_date").alias("valid_from"),
    "account_status",
    "credit_limit",
    lit("9999-12-31").alias("valid_to"),
).withColumn("is_current_flag", lit(True))

# Late-arriving updates: predates the current active row (identifies historical window to split)
late = joined.where(col("effective_date") < col("current_valid_from")).select(
    batch.account_id, "account_status", "credit_limit", "effective_date"
)

target_window = late.join(
    account_history.select(
        col("account_id").alias("h_account_id"),
        col("valid_from").alias("h_valid_from"),
        col("valid_to").alias("h_valid_to"),
        col("account_status").alias("h_status"),
        col("credit_limit").alias("h_credit_limit"),
    ),
    (late.account_id == col("h_account_id")) &
    (col("effective_date") >= col("h_valid_from")) &
    (col("effective_date") < col("h_valid_to"))
).where((col("account_status") != col("h_status")) | (col("credit_limit") != col("h_credit_limit")))

late_shrink = target_window.select(
    "account_id",
    col("h_valid_from").alias("valid_from"),
    lit(None).cast("string").alias("account_status"),
    lit(None).cast("int").alias("credit_limit"),
    col("effective_date").alias("valid_to"),
)

late_insert = target_window.select(
    "account_id",
    col("effective_date").alias("valid_from"),
    "account_status",
    "credit_limit",
    col("h_valid_to").alias("valid_to"),
).withColumn("is_current_flag", lit(False))

# Unify staging records for atomic Delta MERGE
staged_shrink = normal_shrink.unionByName(late_shrink).withColumn("is_current_flag", lit(False))
staged_insert = normal_insert.unionByName(late_insert)
staged = staged_shrink.unionByName(staged_insert)

# Execute SCD Type 2 MERGE operation
(
    target.alias("t")
    .merge(staged.alias("s"), "t.account_id = s.account_id AND t.valid_from = s.valid_from")
    .whenMatchedUpdate(set={"valid_to": "s.valid_to", "is_current": "s.is_current_flag"})
    .whenNotMatchedInsert(
        values={
            "account_id": "s.account_id",
            "account_status": "s.account_status",
            "credit_limit": "s.credit_limit",
            "valid_from": "s.valid_from",
            "valid_to": "s.valid_to",
            "is_current": "s.is_current_flag",
        }
    )
    .execute()
)

result = spark.table("workspace.gold.account_history")
print(f"total rows: {result.count()}")
print(f"current rows: {result.filter('is_current = true').count()}")