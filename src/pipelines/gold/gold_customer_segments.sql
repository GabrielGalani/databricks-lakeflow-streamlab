CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${layer_gold}.gold_customer_segments
COMMENT 'Segmentação de clientes baseada em comportamento de compra (RFM)'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
    customer_id,
    max(order_purchase_timestamp) as last_purchase,
    datediff(current_date(), max(order_purchase_timestamp)) as recency_days,
    count(distinct order_id) as frequency,
    sum(total_payment_value) as monetary_value
FROM ${catalog}.${layer_silver}.silver_orders_enriched
GROUP BY 1;