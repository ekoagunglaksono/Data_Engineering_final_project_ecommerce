{{ config(materialized='table') }}

with dedup as (
    select *,
           row_number() over(partition by user_id order by created_at desc) as rn
    from {{ source('raw', 'users') }}
)

select
  SAFE_CAST(user_id AS INT64) as user_id,
  TRIM(COALESCE(name, '')) as name,
  LOWER(TRIM(COALESCE(email, ''))) as email,
  REGEXP_REPLACE(COALESCE(address, ''), r'\n', ', ') as address,
  SAFE_CAST(created_at AS DATE) as created_date
from dedup
where rn = 1
