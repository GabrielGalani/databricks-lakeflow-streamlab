CREATE OR REFRESH MATERIALIZED VIEW gold_seller_quality
COMMENT 'Ranking e reputação de vendedores'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
    s.seller_id,
    s.seller_state,
    count(distinct oi.order_id) as total_sales,
    avg(r.review_score) as avg_rating,
    count(case when r.review_score = 1 then 1 end) as total_bad_reviews
FROM olist_lakehouse.silver.silver_sellers s
INNER JOIN olist_lakehouse.silver.silver_orders_items oi ON s.seller_id = oi.seller_id
LEFT JOIN olist_lakehouse.silver.silver_order_reviews r ON oi.order_id = r.order_id
GROUP BY 1, 2;