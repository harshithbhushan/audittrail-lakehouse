# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.landing;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.landing.raw_files;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.bronze;

# COMMAND ----------

from typing import TYPE_CHECKING
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat, lit, substring
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Type hint for local IDEs/linters (VS Code); Databricks injects 'spark' at runtime
if TYPE_CHECKING:
    spark: SparkSession = None  # type: ignore

# 1. Parameterize file input using Databricks Widgets (Default: transactions.csv)
dbutils.widgets.text("batch_file", "transactions.csv")
batch_file = dbutils.widgets.get("batch_file")

# 2. Explicit schema matching data contract specification (prevents inferSchema overhead)
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

# 3. Read raw CSV landing volume dynamically via widget parameter
raw = spark.read.option("header", True).schema(schema).csv(
    f"/Volumes/workspace/landing/raw_files/{batch_file}"
)

# 4. Native JVM PII Masking:
# - Full SHA-256 hashing on customer_name
# - Partial mask on SSN (first two digits masked, last two preserved for fraud analysis)
# - account_id remains untouched (internal surrogate key needed for joins)
masked = (
    raw
    .withColumn("customer_name", sha2(col("customer_name"), 256))
    .withColumn("ssn_last4", concat(lit("XX"), substring(col("ssn_last4"), 3, 2)))
)

# 5. Incremental Append to Bronze Delta Table
masked.write.format("delta").mode("append").saveAsTable("workspace.bronze.transactions")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM workspace.bronze.transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.bronze.transactions;