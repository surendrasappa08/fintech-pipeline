{{ config(materialized='table') }}

SELECT
    merchant,
    category,
    status,
    COUNT(*) AS total_transactions,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    MIN(amount) AS min_amount,
    MAX(amount) AS max_amount
FROM {{ ref('stg_transactions') }}
GROUP BY merchant, category, status
ORDER BY total_amount DESC
