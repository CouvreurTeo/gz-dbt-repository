WITH subquery_b AS (
    SELECT
        orders_id,
        SUM(shipping_fee) AS shipping_fee_per_order,
        SUM(logcost) AS logcost_per_order,
        SUM (ship_cost) AS ship_cost_per_order
    FROM {{ ref('stg_raw__ship') }}
    GROUP BY orders_id
),
subquery_c AS (
    SELECT
        orders_id,
        shipping_fee_per_order + logcost_per_order + ship_cost_per_order AS operational_cost_per_order
    FROM subquery_b
)
SELECT
    iom.orders_id,
    iom.date_date,
    iom.margin_per_order,
    sqc.operational_cost_per_order,
    ROUND((iom.margin_per_order - sqc.operational_cost_per_order), 2) AS operational_margin_per_order
FROM {{ ref('int_orders_margin') }} AS iom
INNER JOIN subquery_c AS sqc
ON iom.orders_id = sqc.orders_id