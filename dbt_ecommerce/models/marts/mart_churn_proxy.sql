{{ config(materialized='table') }}

with last_orders as (
    select
        u.user_id,
        max(f.order_date) as last_order_date
    from {{ ref('dim_users') }} u
    left join {{ ref('fact_orders') }} f on u.user_id = f.user_id
    group by u.user_id
)

select
    current_date() as snapshot_date,
    countif(date_diff(current_date(), last_order_date, day) > 90) as churned_customers,
    countif(date_diff(current_date(), last_order_date, day) <= 90) as active_customers,
    round(100 * countif(date_diff(current_date(), last_order_date, day) > 90) / count(*), 2) as churn_rate_pct
from last_orders
