-- thin staging model: renames + casts only, no business logic.
-- valid_from/valid_to are stored as strings in gold.account_history (a
-- consequence of the MERGE logic's string-literal sentinel date); cast
-- to real dates here so every downstream consumer gets a proper type
-- instead of each one casting it independently
select
    account_id,
    account_status,
    cast(credit_limit as decimal(18, 2)) as credit_limit,
    cast(valid_from as date) as valid_from,
    cast(valid_to as date) as valid_to,
    is_current
from {{ source('gold', 'account_history') }}