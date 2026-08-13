# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.landing.gx_store;

# COMMAND ----------

# MAGIC %pip install great_expectations==1.20.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import shutil
import os
from typing import TYPE_CHECKING
import great_expectations as gx
from pyspark.sql import SparkSession

# Type hint for local IDEs/linters (VS Code); Databricks injects 'spark' at runtime
if TYPE_CHECKING:
    spark: SparkSession = None  # type: ignore

context = gx.get_context(mode="file", project_root_dir="/Volumes/workspace/landing/gx_store")

silver_df = spark.table("workspace.silver.transactions")

# Helper functions for idempotent asset and batch definition registration
def get_or_add_asset(datasource, name):
    try:
        return datasource.get_asset(name)
    except LookupError:
        return datasource.add_dataframe_asset(name=name)

def get_or_add_batch_definition(asset, name):
    try:
        return asset.get_batch_definition(name)
    except LookupError:
        return asset.add_batch_definition_whole_dataframe(name)

# 1. Register Data Source, Asset, and Batch Definition
data_source = context.data_sources.add_or_update_spark(name="spark_silver", persist=False)
data_asset = get_or_add_asset(data_source, "silver_transactions")
batch_definition = get_or_add_batch_definition(data_asset, "silver_transactions_batch")

# 2. Define Great Expectations Suite
suite = context.suites.add_or_update(gx.ExpectationSuite(name="silver_transactions_suite"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="transaction_id"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="account_id"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="amount"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="amount", min_value=0.01, max_value=1_000_000))
suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(column="currency", regex="^[A-Z]{3}$"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="transaction_id"))

# 3. Create Validation Definition and Checkpoint
validation_definition = context.validation_definitions.add_or_update(
    gx.ValidationDefinition(name="silver_transactions_validation", data=batch_definition, suite=suite)
)

checkpoint = context.checkpoints.add_or_update(
    gx.Checkpoint(
        name="silver_transactions_checkpoint",
        validation_definitions=[validation_definition],
        actions=[gx.checkpoint.actions.UpdateDataDocsAction(name="update_all_data_docs")],
        result_format={"result_format": "COMPLETE"},
    )
)

# 4. Run Checkpoint on Valid Silver Table
result = checkpoint.run(batch_parameters={"dataframe": silver_df})
print(f"success: {result.success}")

# Raise exception on failure for downstream pipeline/CI orchestration
if not result.success:
    validation_result = list(result.run_results.values())[0]
    failed = [r.expectation_config.type for r in validation_result.get_failed_validation_results().results]
    raise RuntimeError(f"Data quality check failed: {failed}")

# COMMAND ----------

from pyspark.sql.functions import col, lit, when

# Deliberately corrupt an in-memory DataFrame to test failure assertions
broken_df = silver_df.limit(100)
broken_df = broken_df.withColumn(
    "amount",
    when(col("transaction_id") == broken_df.first()["transaction_id"], lit(-50.0)).otherwise(col("amount"))
)
broken_df = broken_df.withColumn(
    "currency",
    when(col("transaction_id") == broken_df.collect()[1]["transaction_id"], lit("usd")).otherwise(col("currency"))
)

result_broken = checkpoint.run(batch_parameters={"dataframe": broken_df})
print(f"success: {result_broken.success}")

validation_result = list(result_broken.run_results.values())[0]
failed = [r.expectation_config.type for r in validation_result.get_failed_validation_results().results]
print(f"failed expectations: {failed}")

try:
    if not result_broken.success:
        raise RuntimeError(f"Data quality check failed: {failed}")
except RuntimeError as e:
    print(f"raised as expected: {e}")

# COMMAND ----------

# Archive Data Docs report bundle to Volume for download
local_tmp_path = "/tmp/data_docs_report"
archive_path = shutil.make_archive(
    local_tmp_path,
    "zip",
    "/Volumes/workspace/landing/gx_store/gx/uncommitted/data_docs/local_site",
)
print(f"created locally at: {archive_path}")

volume_dest = "/Volumes/workspace/landing/gx_store/data_docs_report.zip"
shutil.copy(archive_path, volume_dest)
print(f"copied to volume: {volume_dest}")
print(f"file exists on volume: {os.path.exists(volume_dest)}")
print(f"file size: {os.path.getsize(volume_dest)} bytes")