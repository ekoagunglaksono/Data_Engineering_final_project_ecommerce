{{ config(materialized='table') }}

with orders as (
    select *
    from {{ ref('stg_orders') }}
),

-- Agregasi order_items per order
order_items as (
    select
        order_id,
        sum(quantity) as total_quantity,
        sum(line_total) as order_items_total
    from {{ ref('stg_order_items') }}
    group by order_id
),

-- Agregasi payments per order
payments as (
    select
        order_id,
        sum(amount) as total_payment,
        count(*) as payment_count,
        max(case when is_success then 1 else 0 end) as has_success_payment
    from {{ ref('stg_payments') }}
    group by order_id
),

-- Agregasi shipments per order
shipments as (
    select
        order_id,
        count(*) as shipment_count,
        avg(shipping_cost) as avg_shipping_cost
    from {{ ref('stg_shipments') }}
    group by order_id
)

select
    o.order_id,
    o.user_id,
    o.order_date,
    o.status,
    coalesce(oi.total_quantity, 0) as total_quantity,
    coalesce(oi.order_items_total, 0) as order_total,
    coalesce(p.total_payment, 0) as total_payment,
    coalesce(p.payment_count, 0) as payment_count,
    coalesce(p.has_success_payment, 0) as has_success_payment,
    coalesce(s.shipment_count, 0) as shipment_count,
    coalesce(s.avg_shipping_cost, 0) as avg_shipping_cost
from orders o
left join order_items oi on o.order_id = oi.order_id
left join payments p on o.order_id = p.order_id
left join shipments s on o.order_id = s.order_id
