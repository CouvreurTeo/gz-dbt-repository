SELECT
    orders_id,
    date_date,
    SUM (revenue) AS revenue_per_order,
    SUM (quantity) AS quantity_per_order,
    SUM (purchase_cost) AS purchase_cost_per_order,
    SUM (margin) AS margin_per_order
FROM {{ ref('int_sales_margin') }}
GROUP BY orders_id, date_date
