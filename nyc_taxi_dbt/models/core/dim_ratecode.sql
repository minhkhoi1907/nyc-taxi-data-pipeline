{{ config(materialized='table') }}

SELECT 1 AS ratecode_id, 'Standard rate' AS rate_description, false AS is_airport UNION ALL
SELECT 2, 'JFK', true UNION ALL
SELECT 3, 'Newark', true UNION ALL
SELECT 4, 'Nassau or Westchester', false UNION ALL
SELECT 5, 'Negotiated fare', false UNION ALL
SELECT 6, 'Group ride', false UNION ALL
SELECT 99, 'Unknown', false
