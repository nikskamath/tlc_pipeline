-- models/staging/stg_yellow_trips.sql
-- Databricks: Silver layer already does cleaning in PySpark notebooks,
-- this model is a thin pass-through with column renaming for dbt's
-- naming convention.

select
    pickup_ts,
    dropoff_ts,
    trip_duration_mins,
    trip_distance,
    passenger_count,
    PULocationID as pulocationid,
    DOLocationID as dolocationid,
    fare_amount,
    tip_amount,
    tolls_amount,
    congestion_surcharge,
    coalesce(airport_fee, 0)        as airport_fee,
    coalesce(cbd_congestion_fee, 0) as cbd_congestion_fee,
    total_amount,
    payment_type,
    pickup_borough,
    pickup_zone,
    current_timestamp() as _dbt_loaded_at
from {{ source('silver', 'yellow_trips_clean') }}
where fare_amount   > {{ var('min_fare') }}
  and trip_distance > 0
