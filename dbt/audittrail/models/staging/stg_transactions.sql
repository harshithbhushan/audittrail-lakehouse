-- thin staging model: renames + casts only, no business logic.
-- reads from Silver, not Bronze -- this data already passed the Day 2
-- contract and Day 4 dead-letter routing; re-validating it here would be
-- redundant, not extra-safe
select
    transaction_id,
    account_id,
    customer_name,
    ssn_last4,
    cast(amount as decimal(18, 2)) as amount,
    currency,
    transaction_type,
    event_timestamp as transacted_at,
    ingestion_timestamp as ingested_at
from {{ source('silver', 'transactions') }}