create or refresh streaming live table ${catalog}.silver.silver_customers
(
  constraint valid_customer_id expect (customer_id is not null and length(trim(customer_id)) = 32) on violation drop row,
  constraint valid_customer_unique_id expect (customer_unique_id is not null and length(trim(customer_unique_id)) = 32) on violation drop row,
  constraint valid_zip_code expect (customer_zip_code_prefix is not null) on violation drop row
)
comment 'silver layer - cleaned, validated and deduplicated customers (latest state)'
tblproperties (
  'quality' = 'silver',
  'pipelines.autooptimize.managed' = 'true',
  'pipelines.autooptimize.zordercols' = 'customer_id'
)
as
with deduplicated as (

  select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    initcap(trim(customer_city))  as customer_city,
    upper(trim(customer_state))   as customer_state,
    trim(customer_name)           as customer_name,
    lower(trim(customer_email))   as customer_email,
    trim(customer_phone)          as customer_phone,
    _source_file,
    _file_modified_at,
    _ingested_at,
    _rescued_data,
    row_number() over (partition by customer_id order by _ingested_at desc) as rn

  from stream live.bronze_customers
)

select
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state,
  customer_name,
  customer_email,
  customer_phone,

  _source_file,
  _file_modified_at,
  _ingested_at,
  _rescued_data

from deduplicated
where rn = 1;
