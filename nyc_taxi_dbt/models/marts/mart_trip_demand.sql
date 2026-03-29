{{ config(materialized='table') }}

SELECT
    f.pickup_location_id,
    l.borough,
    l.zone,
    d.year,
    d.month,
    d.day_of_week,
    t.hour,
    t.time_of_day,
    COUNT(f.trip_id) AS total_trips,
    SUM(f.passenger_count) AS total_passengers
FROM {{ ref('fact_trip') }} f
LEFT JOIN {{ ref('dim_date') }} d ON f.pickup_date_id = d.date_id
LEFT JOIN {{ ref('dim_time') }} t ON f.pickup_time_id = t.time_id
LEFT JOIN {{ ref('dim_location') }} l ON f.pickup_location_id = l.location_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
