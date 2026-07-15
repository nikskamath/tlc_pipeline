-- models/marts/agg_daily_revenue.sql
select
    pickup_date as trip_date,
    pickup_borough,
    count(*)                       as total_trips,
    round(sum(total_amount), 2)    as total_revenue,
    round(avg(fare_amount), 2)     as avg_fare,
    round(avg(tip_amount), 2)      as avg_tip,
    round(avg(trip_distance), 2)   as avg_distance_miles,
    round(avg(trip_duration_mins), 2) as avg_duration_mins,
    count_if(payment_type = 1)      as card_trips,
    count_if(payment_type = 2)      as cash_trips
from {{ ref('fct_trips') }}
group by 1, 2
