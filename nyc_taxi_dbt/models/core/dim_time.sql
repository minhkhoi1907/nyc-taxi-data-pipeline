{{ config(materialized='table') }}

WITH time_spine AS (
    -- generate every minute in a day (1440 rows)
    SELECT unnest(generate_series(TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2000-01-01 23:59:00', INTERVAL 1 MINUTE))::TIME AS raw_time
)

SELECT
    -- Format: HHMM string converted to int
    strftime(raw_time, '%H%M')::INT AS time_id,
    strftime(raw_time, '%H') AS hour,
    strftime(raw_time, '%M') AS minute,
    CASE
        WHEN strftime(raw_time, '%H')::INT BETWEEN 5 AND 11 THEN 'Morning'
        WHEN strftime(raw_time, '%H')::INT BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN strftime(raw_time, '%H')::INT BETWEEN 18 AND 22 THEN 'Evening'
        ELSE 'Night'
    END AS time_of_day
FROM time_spine
