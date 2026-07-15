-- tests/assert_no_future_trips.sql
-- Data contract: fails the run if any pickup timestamps are in
-- the future, which would indicate a timezone or data quality bug
-- upstream. Returns 0 rows when passing.

select
    pickup_ts,
    fare_amount,
    pulocationid
from {{ ref('fct_trips') }}
where pickup_ts > current_timestamp()
