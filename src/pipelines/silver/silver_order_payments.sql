create or refresh streaming live table ${catalog}.${layer_silver}.silver_order_payments
(
  constraint valid_order_id expect (order_id is not null and length(trim(order_id)) = 32) on violation drop row,
  constraint valid_payment_sequential expect (payment_sequential is not null and payment_sequential > 0) on violation drop row,
  constraint valid_payment_type expect (payment_type is not null and trim(payment_type) <> '') on violation drop row,
  constraint valid_installments expect (payment_installments is not null and payment_installments >= 1) on violation drop row,
  constraint valid_payment_value expect (payment_value is not null and payment_value >= 0) on violation drop row
)
comment 'silver layer - cleansed order payments data (1 row per payment)'
tblproperties (
    'quality' = 'silver',
    'pipelines.autooptimize.managed' = 'true',
    'pipelines.autooptimize.zordercols' = 'order_id'
)
as
select
    order_id,
    cast(payment_sequential as int) as payment_sequential,
    lower(trim(payment_type)) as payment_type,
    cast(payment_installments as int) as payment_installments,
    cast(payment_value as double) as payment_value,
    case
      when lower(trim(payment_type)) = 'credit_card' 
        then true
        else false
      end 
    as is_credit_card,
    _source_file,
    _file_modified_at,
    _ingested_at,
    _rescued_data,
    current_timestamp() as _processed_at
from stream ${catalog}.${layer_bronze}.bronze_order_payments
where _rescued_data is null;
