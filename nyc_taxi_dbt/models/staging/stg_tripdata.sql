{{ config(materialized='table') }}

WITH raw_taxi_data AS (
    SELECT *
    FROM read_parquet('{{ var("raw_data_path") }}', union_by_name=true)
),

renamed AS (
    SELECT 
        COALESCE(
            TRY_CAST(VendorID AS VARCHAR),
            TRY_CAST(vendorid AS VARCHAR)
        ) AS vendor_id,
        
        TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
        TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
        
        TRY_CAST(passenger_count AS DOUBLE) AS passenger_count,
        TRY_CAST(trip_distance AS DOUBLE) AS trip_distance,
        
        COALESCE(
            TRY_CAST(PULocationID AS INT),
            TRY_CAST(pulocationid AS INT)
        ) AS pickup_location_id,
        
        COALESCE(
            TRY_CAST(DOLocationID AS INT),
            TRY_CAST(dolocationid AS INT)
        ) AS dropoff_location_id,
        
        COALESCE(
            TRY_CAST(RatecodeID AS INT),
            TRY_CAST(ratecodeid AS INT)
        ) AS ratecode_id,
        
        TRY_CAST(payment_type AS INT) AS payment_id,
        
        TRY_CAST(fare_amount AS DOUBLE) AS fare_amount,
        
        COALESCE(TRY_CAST(extra AS DOUBLE), 0) + 
        COALESCE(TRY_CAST(improvement_surcharge AS DOUBLE), 0) + 
        COALESCE(TRY_CAST(congestion_surcharge AS DOUBLE), 0) + 
        COALESCE(TRY_CAST(Airport_fee AS DOUBLE), 0) +
        COALESCE(TRY_CAST(airport_fee AS DOUBLE), 0) +
        COALESCE(TRY_CAST(mta_tax AS DOUBLE), 0) AS surcharge,
        
        COALESCE(TRY_CAST(mta_tax AS DOUBLE), 0) AS mta_tax,
        
        TRY_CAST(tip_amount AS DOUBLE) AS tip_amount,
        TRY_CAST(tolls_amount AS DOUBLE) AS tolls_amount,
        TRY_CAST(total_amount AS DOUBLE) AS total_amount

    FROM raw_taxi_data

    -- Apply Data Quality Filters
    WHERE 
        pickup_datetime < dropoff_datetime
        AND (tpep_pickup_datetime IS NOT NULL)
        AND passenger_count > 0 
        AND trip_distance >= 0
        AND fare_amount >= 0
        AND (pickup_location_id IS NULL OR pickup_location_id BETWEEN 1 AND 265)
        AND (dropoff_location_id IS NULL OR dropoff_location_id BETWEEN 1 AND 265)
)

SELECT
    MD5(CONCAT(vendor_id, pickup_datetime::VARCHAR, dropoff_datetime::VARCHAR, pickup_location_id::VARCHAR)) AS trip_id,
    *,
    DATE_DIFF('second', pickup_datetime, dropoff_datetime) AS trip_duration
FROM renamed
