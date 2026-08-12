# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.silver;

# COMMAND ----------

from typing import TYPE_CHECKING
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, array, array_join, filter as sfilter, size

# Type hint for local IDEs/linters (VS Code); Databricks injects 'spark' at runtime
if TYPE_CHECKING:
    spark: SparkSession = None  # type: ignore

# 1. Parameterize Silver run to track batch metrics (Default: transactions.csv)
dbutils.widgets.text("batch_file", "transactions.csv")
batch_file = dbutils.widgets.get("batch_file")

# 2. Load Bronze dataset
bronze = spark.table("workspace.bronze.transactions")

# 3. Quality Rule Evaluation: One slot per rule (null if passed, reason string if failed).
# Combining reasons preserves full triage context when multiple validation rules fail.
reasons = array(
    when(col("amount").isNull() | (col("amount") <= 0), lit("amount_invalid")),
    when(~col("currency").rlike("^[A-Z]{3}$"), lit("currency_invalid")),
    when(col("transaction_id").isNull(), lit("transaction_id_invalid")),
)
reason_list = sfilter(reasons, lambda x: x.isNotNull())

tagged = (
    bronze
    .withColumn("rejection_reason", array_join(reason_list, "; "))
    .withColumn("_reason_count", size(reason_list))
)

# 4. Separate clean records from dead letter records
valid = tagged.filter(col("_reason_count") == 0).drop("rejection_reason", "_reason_count")
dead_letter = tagged.filter(col("_reason_count") > 0).drop("_reason_count")

# 5. Overwrite Silver Delta tables with the current full validated state
valid.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.transactions")
dead_letter.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.transactions_dead_letter")

# 6. Cumulative totals output
total = bronze.count()
n_valid = valid.count()
n_dead = dead_letter.count()
print(f"CUMULATIVE METRICS -- total: {total} | valid: {n_valid} | dead_letter: {n_dead}")

# 7. Batch-Specific Summary (Isolates metrics for the target input file only)
batch_ids = spark.read.option("header", True).csv(
    f"/Volumes/workspace/landing/raw_files/{batch_file}"
).select("transaction_id")

batch_valid = valid.join(batch_ids, "transaction_id", "left_semi").count()
batch_dead = dead_letter.join(batch_ids, "transaction_id", "left_semi").count()
batch_total = batch_valid + batch_dead
rejection_rate = (batch_dead / batch_total * 100) if batch_total else 0.0

print(f"\nBATCH METRICS ({batch_file}) -- total: {batch_total} | written: {batch_valid} | "
      f"dead_letter: {batch_dead} | rejection_rate: {rejection_rate:.1f}%")

print("\nREJECTION REASONS FOR THIS BATCH:")
dead_letter.join(batch_ids, "transaction_id", "left_semi") \
    .groupBy("rejection_reason").count().show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM workspace.silver.transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM workspace.silver.transactions_dead_letter;