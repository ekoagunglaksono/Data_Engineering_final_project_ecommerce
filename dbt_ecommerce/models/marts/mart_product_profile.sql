{{ config(materialized='table') }}

select
    format_date('%Y-%m', created_date) as year_month,
    count(distinct product_id) as total_products,
    count(distinct supplier_id) as total_suppliers,
    round(avg(price), 2) as avg_price,
    countif(created_date >= date_sub(current_date(), interval 30 day)) as new_products_last_30d
from {{ ref('dim_products') }}
group by year_month
order by year_month
