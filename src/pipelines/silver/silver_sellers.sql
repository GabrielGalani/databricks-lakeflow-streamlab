create or refresh streaming live table ${catalog}.${layer_silver}.silver_sellers
(
  constraint valid_seller_id expect (seller_id is not null and length(trim(seller_id)) = 32) on violation drop row,
  constraint valid_seller_state expect (seller_state is not null and trim(seller_state) <> '') on violation drop row,
  constraint valid_seller_city expect (seller_city is not null and trim(seller_city) <> '') on violation drop row
)
comment 'silver layer - cleaned, validated and deduplicated sellers (latest state)'
tblproperties (
  'quality' = 'silver',
  'pipelines.autooptimize.managed' = 'true',
  'pipelines.autooptimize.zordercols' = 'seller_id'
)
as
with deduplicated as (

  select
    seller_id,
    seller_zip_code_prefix,
    initcap(trim(seller_city)) as seller_city,
    upper(trim(seller_state))  as seller_state,
    _source_file,
    _file_modified_at,
    _ingested_at,
    _rescued_data
  from stream ${catalog}.${layer_bronze}.bronze_sellers
)

select
  seller_id,
  seller_zip_code_prefix,
  seller_city,
  seller_state,
  _source_file,
  _file_modified_at,
  _ingested_at,
  _rescued_data

from deduplicated