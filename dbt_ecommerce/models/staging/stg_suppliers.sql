{{ config(materialized='table') }}

with dedup as (
    select *,
           row_number() over(partition by supplier_id order by supplier_id) as rn
    from {{ source('raw', 'suppliers') }}
)

select
  SAFE_CAST(supplier_id AS INT64) as supplier_id,
  TRIM(REGEXP_REPLACE(COALESCE(name, ''), r'\s+', ' ')) as supplier_name
from dedup
where rn = 1
