{{ config(materialized='table') }}

WITH date_spine AS (
    SELECT unnest(generate_series(DATE '2020-01-01', DATE '2030-12-31', INTERVAL 1 DAY)) AS raw_date
)
SELECT
    -- Date ID format YYYYMMDD
    strftime(raw_date, '%Y%m%d')::INT AS date_id,
    raw_date AS full_date,
    strftime(raw_date, '%Y') AS year,
    strftime(raw_date, '%m') AS month,
    strftime(raw_date, '%d') AS day,
    strftime(raw_date, '%q') AS quarter,
    dayname(raw_date) AS day_of_week,
    week(raw_date)::VARCHAR AS week_of_year,
    CASE 
        WHEN dayofweek(raw_date) IN (0, 6) THEN true -- 0=Sunday, 6=Saturday in duckdb
        ELSE false 
    END AS is_weekend
FROM date_spine



