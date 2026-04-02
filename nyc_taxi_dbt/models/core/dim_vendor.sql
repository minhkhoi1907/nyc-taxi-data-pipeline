{{ config(materialized='table') }}

SELECT 1 AS vendor_id, 'Creative Mobile Technologies' AS vendor_name UNION ALL
SELECT 2 AS vendor_id, 'VeriFone Inc.' AS vendor_name
