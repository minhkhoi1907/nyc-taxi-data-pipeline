{{ config(materialized='table') }}

SELECT
    f.pickup_location_id,
    pu.zone AS pickup_zone,
    f.dropoff_location_id,
    do.zone AS dropoff_zone,
    d.day_of_week,
    t.hour,
    
    -- Phân loại quãng đường (đường dài vs ngắn)
    CASE 
        WHEN f.trip_distance < 2 THEN 'Short (< 2 miles)'
        WHEN f.trip_distance BETWEEN 2 AND 5 THEN 'Medium (2-5 miles)'
        ELSE 'Long (> 5 miles)'
    END AS distance_category,
    
    COUNT(f.trip_id) AS total_trips,
    AVG(f.trip_distance) AS avg_distance,
    AVG(f.trip_duration) AS avg_duration_minutes,
    
    -- Tính tốc độ (chỉ tính khi duration > 0)
    AVG(CASE 
        WHEN f.trip_duration > 0 THEN f.trip_distance / (f.trip_duration / 60.0) 
        ELSE NULL 
    END) AS avg_speed_mph

FROM {{ ref('fact_trip') }} f
LEFT JOIN {{ ref('dim_location') }} pu ON f.pickup_location_id = pu.location_id
LEFT JOIN {{ ref('dim_location') }} do ON f.dropoff_location_id = do.location_id
LEFT JOIN {{ ref('dim_date') }} d ON f.pickup_date_id = d.date_id
LEFT JOIN {{ ref('dim_time') }} t ON f.pickup_time_id = t.time_id
GROUP BY 1, 2, 3, 4, 5, 6, 7
