create or refresh streaming live table olist_lakehouse.silver.silver_orders
(
    constraint valid_order_id expect (order_id is not null and length(trim(order_id)) = 32) on violation drop row,
    constraint valid_customer_id expect (customer_id is not null and length(trim(customer_id)) = 32)  on violation drop row,
    constraint valid_order_status expect (order_status is not null)  on violation drop row,
    constraint valid_purchase_timestamp expect (order_purchase_timestamp is not null) on violation drop row
)
comment 'silver layer - cleansed and standardized orders data (1 row per order)'
tblproperties (
    'quality' = 'silver',
    'pipelines.autooptimize.managed' = 'true',
    'pipelines.autooptimize.zordercols' = 'order_id'
)
as
select
    order_id,
    customer_id,
    lower(trim(order_status)) as order_status,
    cast(order_purchase_timestamp as timestamp) as order_purchase_timestamp,
    cast(order_approved_at as timestamp) as order_approved_at,
    cast(order_delivered_carrier_date as timestamp) as order_delivered_carrier_date,
    cast(order_delivered_customer_date as timestamp) as order_delivered_customer_date,
    cast(order_estimated_delivery_date as timestamp) as order_estimated_delivery_date,
    _source_file,
    _file_modified_at,
    _ingested_at,
    _rescued_data,
    current_timestamp() as _processed_at
from stream olist_lakehouse.bronze.bronze_orders
where _rescued_data is null;
