SELECT sales_key, COUNT(1)

FROM {{ref('fact_sales')}}

GROUP BY sales_key

HAVING COUNT(1)>1

LIMIT 1
