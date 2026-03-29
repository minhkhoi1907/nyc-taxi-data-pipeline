{{ config(materialized='table') }}

SELECT 
    LocationID::INTEGER AS location_id,
    Borough AS borough,
    Zone AS zone,
    service_zone AS service_area
FROM {{ ref('taxi_zone_lookup') }}
