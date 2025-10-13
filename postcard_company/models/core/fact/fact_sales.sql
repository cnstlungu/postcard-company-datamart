{{ config(
    materialized = 'incremental',
    unique_key = ['sales_key'],
    schema = 'core',
    on_schema_change ='fail'
) }}

SELECT

    {{ dbt_utils.generate_surrogate_key(
      [ "sales_agent_key", "transaction_id"]
    ) }} AS sales_key,
    customer_key,
    transaction_id,
    product_key,
    channel_key,
    bought_date_key,
    geography_key,
    sales_agent_key,
    total_amount,
    qty,
    commissionpct,
    commissionpaid,
    product_price
FROM {{ ref('staging_transactions') }} t
LEFT JOIN {{ref('dim_sales_agent')}} sa ON t.reseller_id = sa.original_reseller_id
