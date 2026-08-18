/*
================================================================================
BUSINESS USE CASE: Ingestion Delay & Pipeline Anomaly Detection
================================================================================

QUESTION ADDRESSED:
Which transactions experienced unusually high ingestion lag before reaching 
the pipeline? (A key operational signal for identifying upstream network delays, 
batch queue backlogs, or data resends).

DATA PIPELINE CONTEXT & HONEST METRIC BOUNDS:
- Standard Ingestion Baseline:
  Standard batch runs strictly cap normal transaction ingestion lag at ~2 hours.
  (Multi-day late arrival scenarios were explicitly isolated to `account_history`).

- Detected Anomaly Signal (Session 8 Chaos Test):
  During Session 8's duplicate-resend chaos testing, select duplicate records 
  were injected with an extended ingestion lag (2–4 hours).

- Deduplication Behavior:
  Because the Silver deduplication logic retains the most recently ingested record, 
  the surviving rows in `silver.transactions` are precisely those higher-lag versions.
  This query directly surface those genuine delay-survivor records.
================================================================================
*/

SELECT
    transaction_id,
    account_id,
    event_timestamp,
    ingestion_timestamp,
    ROUND((unix_timestamp(ingestion_timestamp) - unix_timestamp(event_timestamp)) / 3600.0, 2) AS lag_hours
FROM workspace.silver.transactions
WHERE (unix_timestamp(ingestion_timestamp) - unix_timestamp(event_timestamp)) > 7200  -- more than 2 hours
ORDER BY lag_hours DESC;
