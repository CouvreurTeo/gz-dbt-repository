SELECT
    fd.date_date,
    fd.operational_margin - icd.ads_cost AS ads_margin,
    fd.average_basket AS average_basket,
    fd.operational_margin,
    icd.ads_cost,
    icd.ads_impression,
    icd.ads_clicks,
    fd.qty_sold,
    fd.revenue,
    fd.purchase_cost,
    fd.margin,
    fd.shipping_fee,
    fd.logcost,
    fd.ship_cost
FROM {{ ref('finance_days') }} AS fd
INNER JOIN {{ ref('int_campaigns_day') }} AS icd
ON fd.date_date = icd.date_date
ORDER BY date_date DESC
