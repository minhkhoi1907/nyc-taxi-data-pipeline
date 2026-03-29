{{ config(materialized='table') }}

SELECT
    trip_id,
    vendor_id,
    
    -- Date and Time IDs for Pickup
    strftime(pickup_datetime, '%Y%m%d')::INT AS pickup_date_id,
    strftime(pickup_datetime, '%H%M')::INT AS pickup_time_id,
    
    -- Date and Time IDs for Dropoff
    strftime(dropoff_datetime, '%Y%m%d')::INT AS dropoff_date_id,
    strftime(dropoff_datetime, '%H%M')::INT AS dropoff_time_id,
    
    pickup_location_id,
    dropoff_location_id,
    payment_id,
    ratecode_id,
    
    passenger_count,
    trip_distance,
    trip_duration,
    
    fare_amount,
    surcharge,
    mta_tax,
    tip_amount,
    tolls_amount,
    total_amount

FROM {{ ref('stg_tripdata') }}
