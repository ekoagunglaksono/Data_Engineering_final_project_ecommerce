{{ config(materialized='table') }}

select
    u.user_id,
    u.name,
    sum(f.order_total) as total_spent,
    count(distinct f.order_id) as total_orders
from {{ ref('fact_orders') }} f
join {{ ref('dim_users') }} u on f.user_id = u.user_id
group by u.user_id, u.name
order by total_spent desc
limit 20
