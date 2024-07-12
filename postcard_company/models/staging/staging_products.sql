{{ config(schema='staging') }}

SELECT  

    product_id, 
    {{ dbt_utils.generate_surrogate_key(
      [ 'product_id']
    ) }} AS product_key,
    name AS product_name, 
    g.id AS geography_key, 
    price AS product_price

FROM {{ref('raw_products')}} e
JOIN {{ref('geography')}} g on g.cityname = e.city

QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY e.loaded_timestamp DESC ) = 1