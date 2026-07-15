-- models/marts/fct_trips_incremental.sql
{{ config(
    materialized     = 'incremental',
    unique_key       = ['pickup_ts', 'pulocationid', 'dolocationid', 'fare_amount'],
    partition_by     = 'pickup_date',
    on_schema_change = 'sync_all_columns'
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

    _dbt_loaded_at,
    current_timestamp() as _processed_at

from {{ ref('stg_yellow_trips') }}

{% if is_incremental() %}
    where pickup_ts > (select max(pickup_ts) from {{ this }})
{% endif %}
