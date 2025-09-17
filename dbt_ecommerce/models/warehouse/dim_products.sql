{{ config(materialized='table') }}

select
  product_id,
  product_name,
  price,
  supplier_id,
  created_date
from {{ ref('stg_products') }}
