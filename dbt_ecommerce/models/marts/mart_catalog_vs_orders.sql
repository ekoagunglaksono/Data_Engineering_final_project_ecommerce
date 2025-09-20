{{ config(materialized='table') }}

select
    d.year_month,
    count(distinct p.product_id) as total_products,
    count(distinct f.order_id) as total_orders
from {{ ref('dim_date') }} d
left join {{ ref('dim_products') }} p
  on extract(year from p.created_date) = d.year
 and extract(month from p.created_date) = d.month
left join {{ ref('fact_orders') }} f
  on extract(year from f.order_date) = d.year
 and extract(month from f.order_date) = d.month
group by d.year_month
order by d.year_month
