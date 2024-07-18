{{ config(schema='staging') }}
WITH 

customers_main AS (

    SELECT 
    {{ dbt_utils.generate_surrogate_key(
      [ '0', 'customer_id']
    ) }} AS customer_key,
    customer_id, 
    first_name AS customer_first_name, 
    last_name AS customer_last_name, 
    email AS customer_email,
    0 AS reseller_id
    
    FROM {{ref('raw_customers')}}

),

customers_reseller_type1  AS (

    SELECT  
        "customer first name" AS customer_first_name, 
        "customer last name" AS customer_last_name ,
        "customer email" AS customer_email,
        "reseller id"::INT AS reseller_id
    FROM
        {{ref('raw_reseller_type1_sales')}}
),

reseller_type1_processed AS (

    SELECT  
        customer_first_name, 
        customer_last_name ,
        customer_email,
        reseller_id,
    {{ dbt_utils.generate_surrogate_key(
      [ "reseller_id", "customer_email"]
    ) }} AS customer_key
    FROM customers_reseller_type1
),

customers_reseller_type2 AS (

    SELECT 
        customer_first_name, 
        customer_last_name, 
        customer_email,
        "reseller-id" AS reseller_id
    FROM 
        {{ref('raw_reseller_type2_sales')}}
), 

reseller_type2_processed AS (

    SELECT 
        customer_first_name, 
        customer_last_name, 
        customer_email,
        reseller_id,
     {{ dbt_utils.generate_surrogate_key(
      [ "reseller_id", "customer_email"]
    ) }} AS customer_key
    FROM customers_reseller_type2
),

customers_union AS (

SELECT reseller_id, customer_key, NULL AS customer_id , customer_first_name, customer_last_name, customer_email  FROM reseller_type1_processed

UNION 

SELECT reseller_id, customer_key, NULL AS customer_id, customer_first_name, customer_last_name, customer_email  FROM reseller_type2_processed

UNION

SELECT 0 AS reseller_id, customer_key, customer_id, customer_first_name, customer_last_name, customer_email  FROM customers_main
)



SELECT 

 customer_key,
 customer_id AS original_customer_id,
 customer_first_name, 
 customer_last_name, 
 customer_email,
 s.sales_agent_key

FROM customers_union c
LEFT JOIN {{ref('dim_sales_agent')}} s ON c.reseller_id = s.original_reseller_id