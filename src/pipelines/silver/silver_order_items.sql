create or refresh streaming live table ${catalog}.${layer_silver}.silver_orders_items
(    
  constraint valid_order_id expect (order_id is not null and length(trim(order_id)) = 32) on violation drop row,
  constraint valid_customer_id expect (customer_id is not null and length(trim(customer_id)) = 32) on violation drop row,
  constraint valid_order_status expect (order_status is not null) on violation drop row,
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
    o.order_id,
    o.customer_id,
    oi.product_id,
    oi.seller_id,
    lower(trim(o.order_status)) as order_status,
    cast(o.order_purchase_timestamp as timestamp) as order_purchase_timestamp,
    cast(o.order_approved_at as timestamp) as order_approved_at,
    cast(o.order_delivered_carrier_date as timestamp) as order_delivered_carrier_date,
    cast(o.order_delivered_customer_date as timestamp) as order_delivered_customer_date,
    cast(o.order_estimated_delivery_date as timestamp) as order_estimated_delivery_date,
    o._source_file,
    o._file_modified_at,
    o._ingested_at,
    o._rescued_data,
    current_timestamp() as _processed_at
from stream ${catalog}.${layer_bronze}.bronze_orders o
left join ${catalog}.${layer_bronze}.bronze_order_items oi on (oi.order_id = o.order_id)
where o._rescued_data is null;
