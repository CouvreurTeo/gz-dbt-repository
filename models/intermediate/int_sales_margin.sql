WITH subquery_a AS (
    SELECT
        rgs.date_date,
        rgs.orders_id,
        rgs.products_id,
        rgs.revenue,
        rgs.quantity,
        rgp.purchase_price,
        ROUND(SAFE_MULTIPLY(quantity, purchase_price), 2) AS purchase_cost
FROM {{ ref('stg_raw__sales') }} AS rgs
INNER JOIN {{ ref('stg_raw__product') }} AS rgp
ON rgs.products_id = rgp.products_id
)
SELECT *,
ROUND((revenue - purchase_cost), 2) AS margin,
FROM subquery_a