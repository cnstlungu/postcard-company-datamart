SELECT *
FROM {{ ref('dim_sales_agent') }}
WHERE commission_pct < 0 OR commission_pct > 1
