import sys
import pandas as pd
import great_expectations as gx

# ephemeral context, plain pandas -- CI runners start fresh every run, so
# none of the Databricks-specific persistence/serverless concerns from
# Session 9 apply here. no Spark, no Volumes, no add_or_update needed.
context = gx.get_context()

df = pd.read_csv("data/transactions.csv")
df["amount"] = df["amount"].astype(float)

data_source = context.data_sources.add_pandas(name="ci_pandas")
data_asset = data_source.add_dataframe_asset(name="transactions")
batch_definition = data_asset.add_batch_definition_whole_dataframe("transactions_batch")

suite = context.suites.add(gx.ExpectationSuite(name="ci_transactions_suite"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="transaction_id"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="account_id"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="amount"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="amount", min_value=0.01, max_value=1_000_000))
suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(column="currency", regex="^[A-Z]{3}$"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="transaction_id"))

validation_definition = context.validation_definitions.add(
    gx.ValidationDefinition(name="ci_transactions_validation", data=batch_definition, suite=suite)
)

checkpoint = context.checkpoints.add(
    gx.Checkpoint(name="ci_transactions_checkpoint", validation_definitions=[validation_definition])
)

result = checkpoint.run(batch_parameters={"dataframe": df})

if not result.success:
    validation_result = list(result.run_results.values())[0]
    failed = [r.expectation_config.type for r in validation_result.get_failed_validation_results().results]
    print(f"FAILED: {failed}")
    sys.exit(1)

print("Great Expectations suite passed.")
sys.exit(0)
