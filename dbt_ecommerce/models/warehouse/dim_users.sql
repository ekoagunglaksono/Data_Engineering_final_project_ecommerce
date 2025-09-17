{{ config(materialized='table') }}

with users as (
    select *
    from {{ ref('stg_users') }}
),
orders as (
    select
        user_id,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from {{ ref('stg_orders') }}
    group by user_id
)

select
    u.user_id,
    u.name,
    u.email,
    u.address,
    u.created_date,
    coalesce(o.first_order_date, u.created_date) as first_order_date,
    o.last_order_date
from users u
left join orders o
    on u.user_id = o.user_id
