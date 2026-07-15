-- models/staging/stg_zone_lookup.sql
select
    locationid   as location_id,
    borough,
    zone,
    service_zone
from {{ source('raw', 'taxi_zone_lookup') }}
where locationid is not null
