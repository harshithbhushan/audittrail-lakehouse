# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.landing;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.landing.raw_files;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.bronze;

# COMMAND ----------

from pyspark.sql.functions import col, sha2, concat, lit, substring
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1. Parameterize file input using Databricks Widgets
dbutils.widgets.text("batch_file", "transactions_batch2_poisoned.csv")
batch_file = dbutils.widgets.get("batch_file")

# explicit schema, not inferSchema -- the contract already tells us exactly
# what these types are, no need to let Spark guess from a sample
schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("customer_name", StringType(), False),
    StructField("ssn_last4", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("event_timestamp", TimestampType(), False),
    StructField("ingestion_timestamp", TimestampType(), False),
])

# 2. Read dynamically using batch_file parameter
raw = spark.read.option("header", True).schema(schema).csv(
    f"/Volumes/workspace/landing/raw_files/{batch_file}"
)

# first two digits hidden, last two visible -- enough for fraud/ops
# pattern-matching without exposing the real value
masked = (
    raw
    .withColumn("customer_name", sha2(col("customer_name"), 256))
    .withColumn("ssn_last4", concat(lit("XX"), substring(col("ssn_last4"), 3, 2)))
)
# account_id untouched -- internal surrogate key, not PII, needed for joins

# 3. APPEND to Bronze (Preserves Batch 1 while adding Batch 2)
masked.write.format("delta").mode("append").saveAsTable("workspace.bronze.transactions")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM workspace.bronze.transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.bronze.transactions;



# COMMAND (deduplication-session08) ----------

from pyspark.sql.functions import col, sha2, concat, lit, substring
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1. Parameterize file input using Databricks Widgets
dbutils.widgets.text("batch_file", "transactions_batch3_duplicates.csv")
batch_file = dbutils.widgets.get("batch_file")

# explicit schema, not inferSchema -- the contract already tells us exactly
# what these types are, no need to let Spark guess from a sample
schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("customer_name", StringType(), False),
    StructField("ssn_last4", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("event_timestamp", TimestampType(), False),
    StructField("ingestion_timestamp", TimestampType(), False),
])
# 2. Read dynamically using batch_file parameter
raw = spark.read.option("header", True).schema(schema).csv(
    f"/Volumes/workspace/landing/raw_files/{batch_file}"
)

# first two digits hidden, last two visible -- enough for fraud/ops
    # pattern-matching without exposing the real value
masked = raw.withColumn("customer_name", sha2(col("customer_name"), 256)).withColumn("ssn_last4", concat(lit("XX"), substring(col("ssn_last4"), 3, 2)))
# account_id untouched -- internal surrogate key, not PII, needed for joins

# 3. APPEND to Bronze (Preserves Batch 1 while adding Batch 2)
masked.write.format("delta").mode("append").saveAsTable("workspace.bronze.transactions")