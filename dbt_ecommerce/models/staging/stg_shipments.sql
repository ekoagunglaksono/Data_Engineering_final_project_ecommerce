{{ config(materialized='table') }}

with dedup as (
    select *,
           row_number() over(partition by shipment_id order by shipment_id) as rn
    from {{ source('raw', 'shipments') }}
)

select
  SAFE_CAST(shipment_id AS INT64) as shipment_id,
  SAFE_CAST(order_id AS INT64) as order_id,
  LOWER(TRIM(COALESCE(shipping_status, 'unknown'))) as shipping_status,
  SAFE_CAST(shipping_cost AS FLOAT64) as shipping_cost
from dedup
where rn = 1
