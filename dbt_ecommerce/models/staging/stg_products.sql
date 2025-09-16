{{ config(materialized='table') }}

with dedup as (
    select *,
           row_number() over(partition by product_id order by created_at desc) as rn
    from {{ source('raw', 'products') }}
)

select
  SAFE_CAST(product_id AS INT64) as product_id,
  TRIM(COALESCE(name, '')) as product_name,
  SAFE_CAST(price AS FLOAT64) as price,
  SAFE_CAST(supplier_id AS INT64) as supplier_id,
  SAFE_CAST(created_at AS DATE) as created_date
from dedup
where rn = 1
