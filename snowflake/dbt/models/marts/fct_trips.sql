-- models/marts/fct_trips.sql
-- Standard full-rebuild fact table. For the incremental version
-- used in production, see fct_trips_incremental.sql

with trips as (
    select * from {{ ref('stg_yellow_trips') }}
),
pickup_zones as (
    select location_id, borough as pickup_borough, zone as pickup_zone, service_zone as pickup_service_zone
    from {{ ref('dim_zones') }}
),
dropoff_zones as (
    select location_id, borough as dropoff_borough, zone as dropoff_zone
    from {{ ref('dim_zones') }}
)

select
    t.pickup_ts,
    t.dropoff_ts,
    t.trip_duration_mins,
    date_trunc('day',  t.pickup_ts) as pickup_date,
    date_trunc('hour', t.pickup_ts) as pickup_hour,
    hour(t.pickup_ts)               as pickup_hour_of_day,
    dayofweek(t.pickup_ts)          as pickup_dow,

    t.pulocationid,
    t.dolocationid,
    pu.pickup_borough,
    pu.pickup_zone,
    pu.pickup_service_zone,
    do.dropoff_borough,
    do.dropoff_zone,

    t.trip_distance,
    t.passenger_count,
    t.ratecodeid,

    t.fare_amount,
    t.tip_amount,
    t.tolls_amount,
    t.congestion_surcharge,
    t.airport_fee,
    t.cbd_congestion_fee,
    t.total_amount,
    t.payment_type,

    case t.payment_type
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided'
        else 'Other'
    end as payment_type_desc,

    round(t.tip_amount / nullif(t.fare_amount, 0) * 100, 2) as tip_pct,

    t.vendorid,
    t._dbt_loaded_at

from trips t
left join pickup_zones  pu on t.pulocationid = pu.location_id
left join dropoff_zones do on t.dolocationid = do.location_id
