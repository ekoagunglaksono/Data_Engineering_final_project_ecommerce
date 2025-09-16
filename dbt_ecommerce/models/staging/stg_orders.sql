{{ config(materialized='table') }}

with dedup as (
    select *,
           row_number() over(partition by order_id order by order_date desc) as rn
    from {{ source('raw', 'orders') }}
)

select
  SAFE_CAST(order_id AS INT64) as order_id,
  SAFE_CAST(user_id AS INT64) as user_id,
  SAFE_CAST(order_date AS DATE) as order_date,
  LOWER(TRIM(COALESCE(status, 'pending'))) as status
from dedup
where rn = 1
