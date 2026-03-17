{{ config(materialized='table') }}

WITH raw_taxi_data AS (
    SELECT *
    FROM read_parquet('{{ var("raw_data_path") }}', union_by_name=true)
)

SELECT 
    COALESCE(vendor_id, vendor_name) AS vendor_id,
    
    COALESCE(pickup_datetime, Trip_Pickup_DateTime) AS pickup_datetime,
    COALESCE(dropoff_datetime, Trip_Dropoff_DateTime) AS dropoff_datetime,
    
    COALESCE(passenger_count, Passenger_Count) AS passenger_count,
    COALESCE(trip_distance, Trip_Distance) AS trip_distance,
    
    COALESCE(payment_type, Payment_Type) AS payment_type,
    COALESCE(fare_amount, Fare_Amt) AS fare_amount,
    COALESCE(tip_amount, Tip_Amt) AS tip_amount,
    COALESCE(tolls_amount, Tolls_Amt) AS tolls_amount,
    COALESCE(total_amount, Total_Amt) AS total_amount

FROM raw_taxi_data
WHERE COALESCE(passenger_count, Passenger_Count) > 0 
  AND COALESCE(trip_distance, Trip_Distance) > 0