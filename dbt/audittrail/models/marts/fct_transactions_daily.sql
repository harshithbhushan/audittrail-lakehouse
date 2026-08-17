-- daily fact table: volumes, average amounts, and a high-value count at
-- the $10,000 SAR threshold established back on Day 1. grain is one row
-- per calendar date.
select
    cast(transacted_at as date) as transaction_date,
    count(*) as transaction_count,
    round(sum(amount), 2) as total_amount,
    round(avg(amount), 2) as avg_amount,
    sum(case when amount > 10000 then 1 else 0 end) as high_value_count,
    round(100.0 * sum(case when amount > 10000 then 1 else 0 end) / count(*), 2) as high_value_pct
from {{ ref('stg_transactions') }}
group by 1
