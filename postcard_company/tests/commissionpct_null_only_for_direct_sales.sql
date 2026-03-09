SELECT *
FROM {{ ref('fact_sales') }}
WHERE (sales_agent_key = 0 AND commissionpct IS NOT NULL)
   OR (sales_agent_key != 0 AND commissionpct IS NULL)
