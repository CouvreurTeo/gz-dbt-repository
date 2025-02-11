WITH subquery_d AS (
SELECT date_date,
COUNT (DISTINCT orders_id) AS nb_transactions,
SUM (revenue_per_order) AS revenue,
SUM (margin_per_order) AS margin,
SUM (purchase_cost_per_order) AS purchase_cost,
SUM (shipping_fee_per_order) AS shipping_fee,
SUM (logcost_per_order) AS logcost,
SUM (operational_margin_per_order)AS operational_margin,
SUM (qty_sold) AS qty_sold,
SUM (ship_cost_per_order) AS ship_cost
FROM {{ ref('int_orders_operational') }}
GROUP BY date_date
ORDER BY date_date DESC
)
SELECT *,
revenue / nb_transactions AS average_basket
FROM subquery_d