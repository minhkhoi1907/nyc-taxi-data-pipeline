{{ config(materialized='table') }}

SELECT
    d.year,
    d.month,
    r.rate_description,
    r.is_airport,
    p.payment_name,
    p.is_cash,
    
    -- Phân loại độ lớn của nhóm khách
    CASE
        WHEN f.passenger_count = 1 THEN 'Solo (1 pax)'
        WHEN f.passenger_count = 2 THEN 'Couple (2 pax)'
        WHEN f.passenger_count >= 3 THEN 'Group (3+ pax)'
        ELSE 'Unknown'
    END AS passenger_group,

    COUNT(f.trip_id) AS total_trips,
    SUM(f.total_amount) AS total_revenue,
    AVG(f.trip_distance) AS avg_distance
FROM {{ ref('fact_trip') }} f
LEFT JOIN {{ ref('dim_date') }} d ON f.pickup_date_id = d.date_id
LEFT JOIN {{ ref('dim_ratecode') }} r ON f.ratecode_id = r.ratecode_id
LEFT JOIN {{ ref('dim_payment') }} p ON f.payment_id = p.payment_id
GROUP BY 1, 2, 3, 4, 5, 6, 7
