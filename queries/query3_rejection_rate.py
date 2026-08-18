# ==============================================================================
# BUSINESS USE CASE: Ingestion Batch Data Quality Comparison
# ==============================================================================
#
# PURPOSE:
# Compares data quality metrics between two distinct ingestion batches:
#   1. The original baseline batch (Day 1 - Clean)
#   2. The injected anomaly batch (Day 5 - Chaos Test)
#
# OBJECTIVE:
# Demonstrates that the dead-letter queue dynamically isolates and flags
# problematic sources rather than merely outputting a single static aggregate metric.
#
# METHODOLOGY (Session 5 Pattern Reuse):
# Because the `dead_letter` table lacks an explicit `source_batch` metadata column,
# batch-level isolation is achieved via a left-semi join against each batch's known
# `transaction_id` keys—reusing the batch-summary logging design pattern built in Session 5.
# ==============================================================================

from pyspark.sql import SparkSession

# Initialize spark reference for local IDE autocomplete & linter resolution
try:
    spark  # type: ignore # Checks if spark is already injected by Databricks
except NameError:
    spark = SparkSession.builder.getOrCreate()
    
batch1_ids = spark.read.option("header", True).csv(
    "/Volumes/workspace/landing/raw_files/transactions.csv"
).select("transaction_id")

batch2_ids = spark.read.option("header", True).csv(
    "/Volumes/workspace/landing/raw_files/transactions_batch2_poisoned.csv"
).select("transaction_id")

dead_letter = spark.table("workspace.silver.transactions_dead_letter")
valid = spark.table("workspace.silver.transactions")

def rejection_rate(batch_ids, label):
    n_valid = valid.join(batch_ids, "transaction_id", "left_semi").count()
    n_dead = dead_letter.join(batch_ids, "transaction_id", "left_semi").count()
    total = n_valid + n_dead
    rate = n_dead / total * 100 if total else 0.0
    print(f"{label}: total={total} | valid={n_valid} | dead_letter={n_dead} | rejection_rate={rate:.1f}%")

rejection_rate(batch1_ids, "Batch 1 (Day 1, clean)")
rejection_rate(batch2_ids, "Batch 2 (Day 5, poisoned)")
