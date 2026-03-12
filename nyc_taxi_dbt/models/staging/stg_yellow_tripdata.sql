{{ config(materialized='table') }}

WITH raw_taxi_data AS (
    SELECT *
    FROM read_parquet('{{ var("raw_data_path") }}')
)

SELECT *
FROM raw_taxi_data