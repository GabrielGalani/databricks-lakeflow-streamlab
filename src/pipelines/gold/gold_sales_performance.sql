CREATE OR REFRESH MATERIALIZED VIEW gold_sales_performance
COMMENT 'Performance de vendas agregada por categoria e localização'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
    p.product_category_name,
    c.customer_state,
    c.customer_city,
    date_trunc('month', o.order_purchase_timestamp) as sales_month,
    count(distinct o.order_id) as total_orders,
    sum(p_agg.total_payment_value) as total_revenue,
    avg(p_agg.total_payment_value) as ticket_medio
FROM olist_lakehouse.silver.silver_orders o
INNER JOIN olist_lakehouse.silver.silver_customers c ON o.customer_id = c.customer_id
-- Usando a sua view enriquecida ou agregando diretamente
INNER JOIN (
    SELECT order_id, sum(payment_value) as total_payment_value 
    FROM olist_lakehouse.silver.silver_order_payments 
    GROUP BY 1
) p_agg ON o.order_id = p_agg.order_id
INNER JOIN (
    SELECT oi.order_id, pr.product_category_name
    FROM olist_lakehouse.silver.silver_orders_items oi
    JOIN olist_lakehouse.silver.silver_products pr ON oi.product_id = pr.product_id
) p ON o.order_id = p.order_id
WHERE o.order_status = 'delivered'
GROUP BY 1, 2, 3, 4;