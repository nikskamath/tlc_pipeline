-- models/staging/stg_yellow_trips.sql
with source as (
    select * from {{ source('raw', 'yellow_trips_raw') }}
),

cleaned as (
    select
        TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR(tpep_pickup_datetime))        as pickup_ts,
        TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR(tpep_dropoff_datetime))       as dropoff_ts,
        trip_distance,
        passenger_count,
        pulocationid,
        dolocationid,
        ratecodeid,
        store_and_fwd_flag,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        coalesce(airport_fee, 0)        as airport_fee,
        coalesce(cbd_congestion_fee, 0) as cbd_congestion_fee,
        total_amount,
        payment_type,
        vendorid,
        current_timestamp()             as _dbt_loaded_at
    from source
    -- Use TRY_TO_TIMESTAMP_NTZ to avoid connector/server conversion errors from corrupted timestamp values
    where fare_amount     >= {{ var('min_fare') }}
      and trip_distance   > 0
      and passenger_count between 1 and 10
)

, normalized as (
    select
        case when pickup_ts is not null and extract(year from pickup_ts) between 2009 and extract(year from current_date()) + 1 then pickup_ts else null end as pickup_ts,
        case when dropoff_ts is not null and extract(year from dropoff_ts) between 2009 and extract(year from current_date()) + 1 then dropoff_ts else null end as dropoff_ts,
        trip_distance,
        passenger_count,
        pulocationid,
        dolocationid,
        ratecodeid,
        store_and_fwd_flag,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        coalesce(airport_fee, 0)        as airport_fee,
        coalesce(cbd_congestion_fee, 0) as cbd_congestion_fee,
        total_amount,
        payment_type,
        vendorid,
        current_timestamp()             as _dbt_loaded_at
    from cleaned
)

, cleaned_duration as (
    select *, datediff('minute', pickup_ts, dropoff_ts) as trip_duration_mins
    from normalized
)

select * from cleaned_duration
where -- Relaxed filters to allow more rows into downstream marts (allow null timestamps)
  trip_distance >= 0
  and passenger_count between 1 and 10
  and fare_amount >= 0

