SELECT *, 
    SAFE_CAST(purchse_price AS FLOAT64) AS purchase_price
FROM {{ ref('stg_raw__product') }}
