-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS workspace.silver; 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col, when, lit, array, array_join, filter as sfilter, size
-- MAGIC
-- MAGIC bronze = spark.table("workspace.bronze.transactions")
-- MAGIC
-- MAGIC # one slot per rule: null if it passed, a reason string if it failed.
-- MAGIC # combined instead of stopping at the first failure so a row that's
-- MAGIC # wrong in two ways keeps both reasons on record
-- MAGIC
-- MAGIC reasons = array(
-- MAGIC     when(col("amount").isNull() | (col("amount") <= 0), lit("amount_invalid")),
-- MAGIC     when(~col("currency").rlike("^[A-Z]{3}$"), lit("currency_invalid")),
-- MAGIC     when(col("transaction_id").isNull(), lit("transaction_id_invalid")),
-- MAGIC )
-- MAGIC reason_list = sfilter(reasons, lambda x: x.isNotNull())
-- MAGIC
-- MAGIC tagged = (
-- MAGIC     bronze
-- MAGIC     .withColumn("rejection_reason", array_join(reason_list, "; "))
-- MAGIC     .withColumn("_reason_count", size(reason_list))
-- MAGIC )
-- MAGIC
-- MAGIC valid = tagged.filter(col("_reason_count") == 0).drop("rejection_reason", "_reason_count")
-- MAGIC dead_letter = tagged.filter(col("_reason_count") > 0).drop("_reason_count")
-- MAGIC
-- MAGIC valid.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.transactions")
-- MAGIC dead_letter.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.transactions_dead_letter")
-- MAGIC
-- MAGIC total = bronze.count()
-- MAGIC n_valid = valid.count()
-- MAGIC n_dead = dead_letter.count()
-- MAGIC print(f"total: {total} | valid: {n_valid} | dead_letter: {n_dead}")

-- COMMAND ----------

SELECT COUNT(*) FROM workspace.silver.transactions;

-- COMMAND ----------

SELECT COUNT(*) FROM workspace.silver.transactions_dead_letter;