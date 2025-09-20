{{ config(materialized='table') }}

select
    d.year_month,
    sum(case when f.order_date = u.first_order_date then f.order_total else 0 end) as new_customer_revenue,
    sum(case when f.order_date > u.first_order_date then f.order_total else 0 end) as returning_customer_revenue,
    count(distinct case when f.order_date = u.first_order_date then f.user_id end) as new_customers,
    count(distinct case when f.order_date > u.first_order_date then f.user_id end) as returning_customers
from {{ ref('fact_orders') }} f
join {{ ref('dim_users') }} u on f.user_id = u.user_id
join {{ ref('dim_date') }} d on f.order_date = d.date
group by d.year_month
order by d.year_month
