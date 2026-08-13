# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.silver;

# COMMAND ----------

dbutils.widgets.remove("batch_file")

# COMMAND ----------

from pyspark.sql.functions import col, when, lit, array, array_join, filter as sfilter, size

# Parameterize Silver run to track batch metrics
dbutils.widgets.text("batch_file", "transactions_batch2_poisoned.csv")
batch_file = dbutils.widgets.get("batch_file")

bronze = spark.table("workspace.bronze.transactions")

# one slot per rule: null if it passed, a reason string if it failed.
# combined instead of stopping at the first failure so a row that's
# wrong in two ways keeps both reasons on record

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

valid = tagged.filter(col("_reason_count") == 0).drop("rejection_reason", "_reason_count")
dead_letter = tagged.filter(col("_reason_count") > 0).drop("_reason_count")

valid.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.transactions")
dead_letter.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.transactions_dead_letter")

total = bronze.count()
n_valid = valid.count()
n_dead = dead_letter.count()
print(f"total: {total} | valid: {n_valid} | dead_letter: {n_dead}")

# Batch-Specific Summary (Isolates metrics for the current batch file only)
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





# COMMAND (deduplication-session08) ----------

from pyspark.sql.functions import col, when, lit, array, array_join, filter as sfilter, size, row_number
from pyspark.sql.window import Window

dbutils.widgets.text("batch_file", "transactions_batch3_duplicates.csv")
batch_file = dbutils.widgets.get("batch_file")

bronze = spark.table("workspace.bronze.transactions")

# dedup BEFORE validation -- a duplicate has to resolve to one row before
# "is this row valid" is even a meaningful question to ask it. keep the
# most recently ingested copy per transaction_id: a later ingestion
# usually means a correction/resend superseding the earlier one
before_dedup = bronze.count()
w = Window.partitionBy("transaction_id").orderBy(col("ingestion_timestamp").desc())
bronze_deduped = bronze.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")
after_dedup = bronze_deduped.count()
print(f"dedup -- before: {before_dedup} | after: {after_dedup} | duplicates removed: {before_dedup - after_dedup}")

# one slot per rule: null if it passed, a reason string if it failed.
# combined instead of stopping at the first failure so a row that's
# wrong in two ways keeps both reasons on record
reasons = array(
    when(col("amount").isNull() | (col("amount") <= 0), lit("amount_invalid")),
    when(~col("currency").rlike("^[A-Z]{3}$"), lit("currency_invalid")),
    when(col("transaction_id").isNull(), lit("transaction_id_invalid")),
)
reason_list = sfilter(reasons, lambda x: x.isNotNull())

tagged = (
    bronze_deduped
    .withColumn("rejection_reason", array_join(reason_list, "; "))
    .withColumn("_reason_count", size(reason_list))
)

valid = tagged.filter(col("_reason_count") == 0).drop("rejection_reason", "_reason_count")
dead_letter = tagged.filter(col("_reason_count") > 0).drop("_reason_count")

valid.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.transactions")
dead_letter.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.transactions_dead_letter")

total = bronze_deduped.count()
n_valid = valid.count()
n_dead = dead_letter.count()
print(f"total: {total} | valid: {n_valid} | dead_letter: {n_dead}")

# batch-specific summary -- isolates just this run's file from the
# cumulative tables above, since overwrite recomputes everything each run
batch_ids = spark.read.option("header", True).csv(
    f"/Volumes/workspace/landing/raw_files/{batch_file}"
).select("transaction_id")

batch_valid = valid.join(batch_ids, "transaction_id", "left_semi").count()
batch_dead = dead_letter.join(batch_ids, "transaction_id", "left_semi").count()
batch_total = batch_valid + batch_dead
rejection_rate = batch_dead / batch_total * 100 if batch_total else 0.0

print(f"this batch ({batch_file}) -- total: {batch_total} | written to silver: {batch_valid} | "
      f"routed to dead letter: {batch_dead} | rejection rate: {rejection_rate:.1f}%")

dead_letter.join(batch_ids, "transaction_id", "left_semi") \
    .groupBy("rejection_reason").count().show(truncate=False)