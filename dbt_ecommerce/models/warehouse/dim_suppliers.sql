{{ config(materialized='table') }}

select
  supplier_id,
  supplier_name
from {{ ref('stg_suppliers') }}
