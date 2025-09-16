{{ config(materialized='table') }}

with dedup as (
    select *,
           row_number() over(partition by review_id order by review_id) as rn
    from {{ source('raw', 'reviews') }}
)

select
  SAFE_CAST(review_id AS INT64) as review_id,
  SAFE_CAST(order_item_id AS INT64) as order_item_id,
  SAFE_CAST(rating AS INT64) as rating,
  NULLIF(TRIM(REGEXP_REPLACE(COALESCE(comment, ''), r'\s+', ' ')), '') as comment
from dedup
where rn = 1
