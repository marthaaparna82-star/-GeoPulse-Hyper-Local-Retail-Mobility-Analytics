-- Keep the spatial join output in a simple staging layer for downstream models.
select
    store_id,
    device_id,
    ping_timestamp
from {{ source('raw', 'store_catchment_pings') }}
