{{ config(materialized='table') }}

select
  payment_id,
  order_id,
  amount,
  payment_date,
  is_success
from {{ ref('stg_payments') }}
