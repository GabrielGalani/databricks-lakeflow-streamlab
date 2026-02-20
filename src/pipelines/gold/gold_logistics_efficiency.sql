CREATE OR REFRESH MATERIALIZED VIEW gold_logistics_efficiency
COMMENT 'Métricas de eficiência logística e atrasos por estado'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
    customer_state,
    count(*) as total_deliveries,
    avg(delivery_days) as avg_delivery_time,
    avg(delivery_delay_days) as avg_delay,
    count(case when delivery_delay_days < 0 then 1 end) as orders_with_delay,
    (count(case when delivery_delay_days < 0 then 1 end) / count(*)) * 100 as percentage_delayed
FROM olist_lakehouse.silver.silver_orders_enriched
WHERE order_status = 'delivered'
  AND delivery_days IS NOT NULL
GROUP BY 1;