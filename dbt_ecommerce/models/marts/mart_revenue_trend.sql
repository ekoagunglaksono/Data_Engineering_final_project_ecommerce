{{ config(materialized='table') }}

select
    d.year_month,
    sum(f.order_total) as total_revenue,
    count(distinct f.order_id) as total_orders,
    count(distinct f.user_id) as unique_customers
from {{ ref('fact_orders') }} f
join {{ ref('dim_date') }} d
  on f.order_date = d.date
group by d.year_month
order by d.year_month
