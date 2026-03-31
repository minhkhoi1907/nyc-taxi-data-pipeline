{{ config(materialized='table') }}

WITH raw_taxi_data AS (
    SELECT *
    FROM read_parquet('{{ var("raw_data_path") }}', union_by_name=true)
)

SELECT 
    vendorid AS vendor_id,
    
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    
    passenger_count,
    trip_distance,
    
    pulocationid AS pickup_location_id,
    dolocationid AS dropoff_location_id,
    
    payment_type,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount

FROM raw_taxi_data
WHERE passenger_count > 0 
  AND trip_distance > 0