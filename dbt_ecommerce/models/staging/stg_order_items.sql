{{ config(materialized='table') }}

with order_items as (
    select
        SAFE_CAST(order_item_id AS INT64) as order_item_id,
        SAFE_CAST(order_id AS INT64) as order_id,
        SAFE_CAST(product_id AS INT64) as product_id,
        SAFE_CAST(quantity AS INT64) as quantity,
        row_number() over (
            partition by order_item_id
            order by order_id, product_id
        ) as rn
    from {{ source('raw', 'order_items') }}
    qualify rn = 1
),

products as (
    select
        SAFE_CAST(product_id AS INT64) as product_id,
        SAFE_CAST(price AS FLOAT64) as price,
        row_number() over (
            partition by product_id
            order by created_at desc
        ) as rn
    from {{ source('raw', 'products') }}
    qualify rn = 1
)

select
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    oi.quantity,
    p.price,
    SAFE_CAST(oi.quantity AS FLOAT64) * COALESCE(p.price, 0) as line_total
from order_items oi
left join products p
    on oi.product_id = p.product_id
