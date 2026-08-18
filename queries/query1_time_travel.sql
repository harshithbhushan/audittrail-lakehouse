/*
================================================================================
BUSINESS USE CASE: Forensic System Auditing via Time Travel
================================================================================

QUESTION ADDRESSED:
What were a specific account's credit limit and status as recorded by the
SYSTEM prior to any SCD2 history being populated, versus its current state?

TECHNICAL DISTINCTION (VERSION AS OF vs. Valid From/To):
- valid_from / valid_to (SCD Type 2):
  Answers "What was business-effective on a given date?"
  Handles business-logic updates, backfills, and late-arriving dimensions.

- VERSION AS OF (Delta / Lakehouse System Versioning):
  Answers "What did the SYSTEM actually record at a specific point in time?"
  Serves forensic and compliance audit requirements by reconstructing the precise
  operational state of the table before subsequent modifications or data corrections.

TIMING & VERSION NOTES:
This implementation compares Version 0 (the initial table state before any
consolidated SCD2 updates were applied during Session 8) against the live table state.
================================================================================
*/

SELECT
    curr.account_id,
    v0.credit_limit  AS credit_limit_at_v0,
    curr.credit_limit AS credit_limit_now,
    v0.account_status  AS status_at_v0,
    curr.account_status AS status_now
FROM workspace.gold.account_history VERSION AS OF 0 AS v0
JOIN (
    SELECT * FROM workspace.gold.account_history WHERE is_current = true
) AS curr
ON v0.account_id = curr.account_id
WHERE v0.credit_limit != curr.credit_limit
   OR v0.account_status != curr.account_status
ORDER BY curr.account_id
LIMIT 10;
