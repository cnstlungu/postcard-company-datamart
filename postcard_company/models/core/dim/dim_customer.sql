{{
config(
materialized = 'table',
unique_key = 'customer_key',
schema = 'core'
)
}}

SELECT 
    customer_key, 
    original_customer_id, 
    customer_first_name, 
    customer_last_name, 
    customer_email, 
    sales_agent_key
FROM {{ref('staging_customers')}}