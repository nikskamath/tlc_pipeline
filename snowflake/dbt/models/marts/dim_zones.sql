-- models/marts/dim_zones.sql
select location_id, borough, zone, service_zone
from {{ ref('stg_zone_lookup') }}
