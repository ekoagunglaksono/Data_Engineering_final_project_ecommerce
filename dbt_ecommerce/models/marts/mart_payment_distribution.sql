{{ config(materialized='table') }}

select
    case when p.is_success then 'Success' else 'Failed' end as payment_status,
    count(*) as total_transactions,
    sum(p.amount) as total_amount
from {{ ref('dim_payments') }} p
group by payment_status
order by total_transactions desc
