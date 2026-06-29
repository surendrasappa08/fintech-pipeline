{{ config(materialized='view') }}

SELECT
    transaction_id,
    user_id,
    amount,
    currency,
    merchant,
    category,
    status,
    timestamp::timestamp_ntz AS transaction_time,
    ingested_at
FROM {{ source('transactions', 'RAW_TRANSACTIONS') }}
WHERE transaction_id IS NOT NULL
