-- snapshots/zone_snapshot.sql
{% snapshot zone_snapshot %}
{{
    config(
        target_schema = 'snapshots',
        unique_key    = 'location_id',
        strategy      = 'check',
        check_cols    = ['borough', 'zone', 'service_zone']
    )
}}

select
    location_id,
    borough,
    zone,
    service_zone
from {{ ref('dim_zones') }}

{% endsnapshot %}
