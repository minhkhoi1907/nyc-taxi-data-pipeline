{{ config(materialized='table') }}

SELECT 1 AS payment_id, 'Credit card' AS payment_name, false AS is_cash UNION ALL
SELECT 2, 'Cash', true UNION ALL
SELECT 3, 'No charge', false UNION ALL
SELECT 4, 'Dispute', false UNION ALL
SELECT 5, 'Unknown', false UNION ALL
SELECT 6, 'Voided trip', false
