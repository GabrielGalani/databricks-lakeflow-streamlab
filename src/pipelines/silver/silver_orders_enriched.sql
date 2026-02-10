create or refresh materialized view ${catalog}.silver.silver_orders_enriched
comment 'silver layer - enriched orders (1 row per order)'
tblproperties ('quality' = 'silver')
as
with order_items_agg as (
    select
        order_id,
        count(*) as total_items,
        sum(price) as total_items_price,
        sum(freight_value) as total_freight_value,
        sum(price + freight_value) as order_items_total_value
    from olist_lakehouse.silver.silver_order_items
    group by order_id
),
payments_agg as (
    select
        order_id,
        count(*) as total_payments,
        sum(payment_value) as total_payment_value,
        max(is_credit_card) as used_credit_card
    from olist_lakehouse.silver.silver_order_payments
    group by order_id
),
reviews_agg as (
    select
        order_id,
        avg(review_score) as avg_review_score,
        max(has_comment) as has_review_comment
    from olist_lakehouse.silver.silver_order_reviews
    group by order_id
)
select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    c.customer_state,
    c.customer_city,
    c.customer_zip_code_prefix,
    c.region as customer_region,
    i.total_items,
    i.total_items_price,
    i.total_freight_value,
    i.order_items_total_value,
    p.total_payments,
    p.total_payment_value,
    p.used_credit_card,
    r.avg_review_score,
    r.has_review_comment,
    datediff(o.order_delivered_customer_date, o.order_purchase_timestamp) as delivery_days,
    datediff(o.order_estimated_delivery_date, o.order_delivered_customer_date) as delivery_delay_days,
    o._ingested_at,
    current_timestamp() as _processed_at

from olist_lakehouse.silver.silver_orders o
left join olist_lakehouse.silver.silver_customers c on (o.customer_id = c.customer_id)
left join order_items_agg i on (o.order_id = i.order_id)
left join payments_agg p on (o.order_id = p.order_id)
left join reviews_agg r on (o.order_id = r.order_id);
