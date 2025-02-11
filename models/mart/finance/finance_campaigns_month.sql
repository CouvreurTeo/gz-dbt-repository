WITH subquery_e AS (   
    SELECT *,
    EXTRACT (MONTH FROM date_date) AS date_month
FROM {{ ref('finance_campaigns_day') }}
)
SELECT date_month,
    SUM (ads_margin) AS ads_margin,
    SUM (average_basket) AS average_basket,
    SUM (operational_margin) AS operational_margin,
    SUM (ads_cost) AS ads_cost,
    SUM (ads_impression) AS ads_impression,
    SUM (ads_clicks) AS ads_clicks,
    SUM (qty_sold) AS qty_sold,
    SUM (revenue) AS revenue,
    SUM (purchase_cost) AS purchase_cost,
    SUM (margin) AS margin,
    SUM (shipping_fee) AS shipping_fee,
    SUM (logcost) AS logcost,
    SUM (ship_cost) AS ship_cost
FROM subquery_e
GROUP BY date_month