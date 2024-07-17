{{ config(schema='raw') }}

SELECT 
    transaction_id, 
    customer_id,
    product_id,
    amount,
    qty::INTEGER AS qty,
    channel_id,
    bought_date, 
    CURRENT_TIMESTAMP AS loaded_timestamp 
FROM {{ source('parquet_input','main') }}