{{ config(materialized='table') }}

-- Count distinct devices in each store catchment for each timestamp hour.
select
    store_id,
    date_trunc('hour', ping_timestamp) as visit_hour,
    count(distinct device_id) as unique_visitors
from {{ ref('stg_store_catchment_pings') }}
group by
    store_id,
    date_trunc('hour', ping_timestamp)
