CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${layer_gold}.gold_payment_methods_evolution
COMMENT 'Tendências de métodos de pagamento por período'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
    payment_type,
    date_trunc('week', _ingested_at) as reference_week,
    count(*) as usage_count,
    sum(payment_value) as total_amount,
    avg(payment_installments) as avg_installments
FROM ${catalog}.${layer_silver}.silver_order_payments
GROUP BY 1, 2;