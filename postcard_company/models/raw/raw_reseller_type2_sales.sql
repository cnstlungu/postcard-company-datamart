{{ config(schema='raw') }}

SELECT 
    productName, 
    qty, 
    totalAmount, 
    salesChannel, 
    customer.firstname AS customer_first_name, 
    customer.lastname AS customer_last_name, 
    customer.email AS customer_email, 
    customer.phone AS customer_phone,
    customer.address AS customer_address,
    customer.city AS customer_city,
    customer.country AS customer_country,
    customer.postal AS customer_postal_code, 
    seriesCity, 
    "Created Date", 
    "reseller-id",
    "transactionID",  
    CURRENT_TIMESTAMP AS loaded_timestamp
FROM {{ source('parquet_input','resellers_type2') }}