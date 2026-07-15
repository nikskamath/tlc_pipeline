-- models/marts/fct_trips.sql
{{ config(
    materialized = 'table',
    partition_by = 'pickup_date',
    cluster_by   = ['pickup_borough', 'payment_type']
) }}

select
    pickup_ts,
    dropoff_ts,
    trip_duration_mins,
    trip_distance,
    passenger_count,
    pulocationid,
    dolocationid,
    fare_amount,
    tip_amount,
    tolls_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    total_amount,
    payment_type,
    pickup_borough,
    pickup_zone,

    date_trunc('DAY', pickup_ts)  as pickup_date,
    hour(pickup_ts)               as pickup_hour_of_day,
    dayofweek(pickup_ts)          as pickup_dow,

    case payment_type
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        else 'Other'
    end as payment_type_desc,

    round(tip_amount / nullif(fare_amount, 0) * 100, 2) as tip_pct,

    _dbt_loaded_at

from {{ ref('stg_yellow_trips') }}
