{{ config(materialized='table') }}

with bounds as (
  select
    coalesce(min(order_date), current_date()) as min_date,
    coalesce(max(order_date), current_date()) as max_date
  from {{ ref('stg_orders') }}
),

dates as (
  select day
  from bounds, unnest(generate_date_array(min_date, max_date)) as day
)

select
  day as date,
  extract(year from day) as year,
  extract(month from day) as month,
  format_date('%Y-%m', day) as year_month,
  extract(week from day) as week_of_year,
  extract(day from day) as day_of_month,
  format_date('%A', day) as weekday
from dates
order by date
