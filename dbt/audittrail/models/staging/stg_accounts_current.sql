-- current state only, one row per account -- stg_accounts carries full
-- SCD2 history (multiple rows per account_id), which isn't the right
-- shape for a foreign-key relationship test or for feeding a dbt
-- snapshot (snapshots expect a current-state source to diff against,
-- not something that's already versioned)
select
    account_id,
    account_status,
    credit_limit,
    valid_from,
    valid_to
from {{ ref('stg_accounts') }}
where is_current = true
