-- models/ai/anomaly_flags.sql
-- Pure SQL z-score anomaly detection. Same approach as the Snowflake
-- version — avoids needing a Python/ML cluster runtime, runs as
-- standard SQL on any Databricks SQL warehouse.

with trips as (
    select * from {{ ref('fct_trips') }}
),

stats as (
    select
        pickup_borough,
        avg(fare_amount)           as mean_fare,
        stddev(fare_amount)        as std_fare,
        avg(trip_distance)         as mean_distance,
        stddev(trip_distance)      as std_distance,
        avg(trip_duration_mins)    as mean_duration,
        stddev(trip_duration_mins) as std_duration
    from trips
    group by pickup_borough
),

scored as (
    select
        t.pickup_ts,
        t.fare_amount,
        t.trip_distance,
        t.trip_duration_mins,
        t.passenger_count,
        t.pickup_borough,
        t.payment_type_desc,

        abs(t.fare_amount        - s.mean_fare)     / nullif(s.std_fare, 0)     as fare_zscore,
        abs(t.trip_distance      - s.mean_distance) / nullif(s.std_distance, 0) as distance_zscore,
        abs(t.trip_duration_mins - s.mean_duration) / nullif(s.std_duration, 0) as duration_zscore

    from trips t
    left join stats s on t.pickup_borough = s.pickup_borough
)

select
    pickup_ts,
    fare_amount,
    trip_distance,
    trip_duration_mins,
    passenger_count,
    pickup_borough,
    payment_type_desc,
    fare_zscore,
    distance_zscore,
    duration_zscore,

    round(
        (coalesce(fare_zscore, 0) + coalesce(distance_zscore, 0) + coalesce(duration_zscore, 0)) / 3,
        2
    ) as anomaly_score,

    case
        when fare_zscore     > 3 then true
        when distance_zscore > 3 then true
        when duration_zscore > 3 then true
        else false
    end as is_anomaly,

    case
        when fare_zscore > 3 and distance_zscore > 3 then 'high fare + long distance'
        when fare_zscore > 3 and duration_zscore > 3 then 'high fare + long duration'
        when fare_zscore     > 3 then 'high fare'
        when distance_zscore > 3 then 'long distance'
        when duration_zscore > 3 then 'long duration'
        else null
    end as anomaly_reason

from scored
