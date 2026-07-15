-- models/marts/agg_hourly_demand.sql
select
    pickup_hour_of_day,
    pickup_dow,
    pickup_borough,
    count(*)                   as total_trips,
    round(avg(fare_amount), 2) as avg_fare
from {{ ref('fct_trips') }}
group by 1, 2, 3
