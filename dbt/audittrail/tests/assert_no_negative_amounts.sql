-- custom test: no negative amounts should ever reach the mart.
-- checked at stg_transactions (the mart's direct input), not the
-- aggregated mart itself, so a failure points at the exact offending
-- transaction_id, not just a suspicious-looking total.
-- dbt convention: this test passes only if the query returns zero rows.
select *
from {{ ref('stg_transactions') }}
where amount < 0
