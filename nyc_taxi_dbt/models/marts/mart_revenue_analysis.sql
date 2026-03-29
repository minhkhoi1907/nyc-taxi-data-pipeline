{{ config(materialized='table') }}

SELECT
    f.pickup_location_id,
    l.borough,
    l.zone,
    t.hour,
    p.payment_name,
    p.is_cash,
    COUNT(f.trip_id) AS total_trips,
    SUM(f.total_amount) AS total_revenue,
    AVG(f.fare_amount) AS avg_fare_amount,
    SUM(f.tip_amount) AS total_tip,
    AVG(f.tip_amount) AS avg_tip_amount,
    SUM(f.tolls_amount) AS total_tolls,
    SUM(f.surcharge) AS total_surcharge,
    -- Tính tỷ lệ tip trên tiền cước
    AVG(CASE WHEN f.fare_amount > 0 THEN f.tip_amount / f.fare_amount ELSE 0 END) AS tip_percentage
FROM {{ ref('fact_trip') }} f
LEFT JOIN {{ ref('dim_time') }} t ON f.pickup_time_id = t.time_id
LEFT JOIN {{ ref('dim_location') }} l ON f.pickup_location_id = l.location_id
LEFT JOIN {{ ref('dim_payment') }} p ON f.payment_id = p.payment_id
GROUP BY 1, 2, 3, 4, 5, 6
