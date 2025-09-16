{{ config(materialized='table') }}

with dedup as (
    select *,
           row_number() over(partition by payment_id order by payment_date desc) as rn
    from {{ source('raw', 'payments') }}
)

select
  SAFE_CAST(payment_id AS INT64) as payment_id,
  SAFE_CAST(order_id AS INT64) as order_id,
  SAFE_CAST(payment_date AS DATE) as payment_date,
  SAFE_CAST(amount AS FLOAT64) as amount,
  case
    when lower(cast(is_success as string)) in ('true','t','1','yes') then true
    else false
  end as is_success
from dedup
where rn = 1
