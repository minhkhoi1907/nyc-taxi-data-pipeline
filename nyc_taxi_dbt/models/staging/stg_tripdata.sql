{{ config(materialized='view') }}

WITH raw_taxi_data AS (
    SELECT *
    FROM read_parquet('{{ var("raw_data_path") }}', union_by_name=true)
)

SELECT 
    -- surrogate key / trip identifier
    md5(
        COALESCE(tpep_pickup_datetime::VARCHAR, pickup_datetime::VARCHAR, Trip_Pickup_DateTime::VARCHAR, '') || 
        COALESCE(tpep_dropoff_datetime::VARCHAR, dropoff_datetime::VARCHAR, Trip_Dropoff_DateTime::VARCHAR, '') || 
        COALESCE(PULocationID::VARCHAR, '')
    ) AS trip_id,
    
    COALESCE(VendorID, vendor_id, vendor_name::INT) AS vendor_id,
    
    COALESCE(tpep_pickup_datetime, pickup_datetime, Trip_Pickup_DateTime) AS pickup_datetime,
    COALESCE(tpep_dropoff_datetime, dropoff_datetime, Trip_Dropoff_DateTime) AS dropoff_datetime,
    
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    
    COALESCE(payment_type, Payment_Type) AS payment_id,
    COALESCE(RatecodeID, RateCodeID) AS ratecode_id,
    
    COALESCE(passenger_count, Passenger_Count) AS passenger_count,
    COALESCE(trip_distance, Trip_Distance) AS trip_distance,
    
    -- calculate duration in minutes
    date_diff('minute', 
        COALESCE(tpep_pickup_datetime, pickup_datetime, Trip_Pickup_DateTime),
        COALESCE(tpep_dropoff_datetime, dropoff_datetime, Trip_Dropoff_DateTime)
    ) AS trip_duration,
    
    COALESCE(fare_amount, Fare_Amt) AS fare_amount,
    COALESCE(extra, 0) + COALESCE(improvement_surcharge, 0) + COALESCE(congestion_surcharge, 0) + COALESCE(Airport_fee, 0) AS surcharge,
    COALESCE(mta_tax, 0) AS mta_tax,
    COALESCE(tip_amount, Tip_Amt) AS tip_amount,
    COALESCE(tolls_amount, Tolls_Amt) AS tolls_amount,
    COALESCE(total_amount, Total_Amt) AS total_amount

FROM raw_taxi_data

-- Apply Data Quality Filters (như feedback của user)
WHERE 
    -- 1. Thời gian phải hợp lý (không âm)
    COALESCE(tpep_pickup_datetime, pickup_datetime, Trip_Pickup_DateTime) < COALESCE(tpep_dropoff_datetime, dropoff_datetime, Trip_Dropoff_DateTime)
    
    -- 2. Độ dài chuyến đi và hành khách
    AND COALESCE(passenger_count, Passenger_Count) > 0 
    AND COALESCE(trip_distance, Trip_Distance) >= 0
    
    -- 3. Cước phí không được âm
    AND COALESCE(fare_amount, Fare_Amt) >= 0
    
    -- 4. Location hợp lệ (Location ID trong khoảng 1-265 theo NYC Taxi format)
    AND PULocationID BETWEEN 1 AND 265
    AND DOLocationID BETWEEN 1 AND 265
