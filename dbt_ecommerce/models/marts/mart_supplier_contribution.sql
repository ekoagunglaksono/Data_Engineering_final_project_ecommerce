{{ config(materialized='table') }}

select
    s.supplier_id,
    count(distinct p.product_id) as total_products,
    round(avg(p.price), 2) as avg_price
from {{ ref('dim_suppliers') }} s
join {{ ref('dim_products') }} p
  on s.supplier_id = p.supplier_id
group by s.supplier_id
order by total_products desc
limit 20
