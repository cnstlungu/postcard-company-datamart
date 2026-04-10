{{ config(schema='staging') }}
WITH 

customers_main AS (

    SELECT 
    {{ dbt_utils.generate_surrogate_key(
      [ '0', 'customer_id']
    ) }} AS customer_key,
    customer_id::BIGINT AS customer_id, 
    first_name AS customer_first_name, 
    last_name AS customer_last_name, 
    email AS customer_email,
    phone_number AS customer_phone,
    address AS customer_address,
    city AS customer_city,
    country AS customer_country,
    postal_code AS customer_postal_code,
    0::BIGINT AS reseller_id
    
    FROM {{ref('raw_customers')}}

),

customers_reseller_type1  AS (

    SELECT  
        "Customer First Name" AS customer_first_name, 
        "Customer Last Name" AS customer_last_name ,
        "Customer Email" AS customer_email,
        "Customer Phone" AS customer_phone,
        "Customer Address" AS customer_address,
        "Customer City" AS customer_city,
        "Customer Country" AS customer_country,
        "Customer Postal Code" AS customer_postal_code,
        "Reseller ID"::BIGINT AS reseller_id
    FROM
        {{ref('raw_reseller_type1_sales')}}
),

reseller_type1_processed AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(
          [ "reseller_id", "customer_email"]
        ) }} AS customer_key,
        customer_first_name,
        customer_last_name,
        customer_email,
        customer_phone,
        customer_address,
        customer_city,
        customer_country,
        customer_postal_code,
        reseller_id
    FROM (
        SELECT
            customer_email,
            reseller_id,
            MIN(customer_first_name) AS customer_first_name,
            MIN(customer_last_name) AS customer_last_name,
            MIN(customer_phone) AS customer_phone,
            MIN(customer_address) AS customer_address,
            MIN(customer_city) AS customer_city,
            MIN(customer_country) AS customer_country,
            MIN(customer_postal_code) AS customer_postal_code
        FROM customers_reseller_type1
        GROUP BY reseller_id, customer_email
    )
),

customers_reseller_type2 AS (

    SELECT 
        customer_first_name, 
        customer_last_name, 
        customer_email,
        customer_phone,
        customer_address,
        customer_city,
        customer_country,
        customer_postal_code,
        "reseller-id"::BIGINT AS reseller_id
    FROM 
        {{ref('raw_reseller_type2_sales')}}
), 

reseller_type2_processed AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(
          [ "reseller_id", "customer_email"]
        ) }} AS customer_key,
        customer_first_name,
        customer_last_name,
        customer_email,
        customer_phone,
        customer_address,
        customer_city,
        customer_country,
        customer_postal_code,
        reseller_id
    FROM (
        SELECT
            customer_email,
            reseller_id,
            MIN(customer_first_name) AS customer_first_name,
            MIN(customer_last_name) AS customer_last_name,
            MIN(customer_phone) AS customer_phone,
            MIN(customer_address) AS customer_address,
            MIN(customer_city) AS customer_city,
            MIN(customer_country) AS customer_country,
            MIN(customer_postal_code) AS customer_postal_code
        FROM customers_reseller_type2
        GROUP BY reseller_id, customer_email
    )
),

customers_union AS (

SELECT 
    reseller_id, 
    customer_key, 
    NULL::BIGINT AS customer_id, 
    customer_first_name, 
    customer_last_name, 
    customer_email,
    customer_phone,
    customer_address,
    customer_city,
    customer_country,
    customer_postal_code
FROM reseller_type1_processed

UNION ALL

SELECT 
    reseller_id, 
    customer_key, 
    NULL::BIGINT AS customer_id, 
    customer_first_name, 
    customer_last_name, 
    customer_email,
    customer_phone,
    customer_address,
    customer_city,
    customer_country,
    customer_postal_code
FROM reseller_type2_processed

UNION ALL

SELECT 
    0::BIGINT AS reseller_id, 
    customer_key, 
    customer_id, 
    customer_first_name, 
    customer_last_name, 
    customer_email,
    customer_phone,
    customer_address,
    customer_city,
    customer_country,
    customer_postal_code
FROM customers_main
)



SELECT 

 customer_key,
 customer_id AS original_customer_id,
 customer_first_name, 
 customer_last_name, 
 customer_email,
 customer_phone,
 customer_address,
 customer_city,
 customer_country,
 customer_postal_code,
 s.sales_agent_key

FROM customers_union c
LEFT JOIN {{ref('dim_sales_agent')}} s ON c.reseller_id = s.original_reseller_id
