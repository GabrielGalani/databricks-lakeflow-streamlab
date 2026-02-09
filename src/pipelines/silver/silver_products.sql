create or refresh streaming live table silver_products
(
  constraint valid_product_id expect (product_id is not null and length(trim(product_id)) = 32) on violation drop row,
  constraint valid_category expect (product_category_name is not null and trim(product_category_name) <> '') on violation drop row,

  constraint valid_dimensions
    expect (
      product_weight_g >= 0
      and product_length_cm >= 0
      and product_height_cm >= 0
      and product_width_cm >= 0
    )
    on violation drop row
)
comment 'silver layer - cleaned, validated and deduplicated products (latest state)'
tblproperties (
  'quality' = 'silver',
  'pipelines.autooptimize.managed' = 'true',
  'pipelines.autooptimize.zordercols' = 'product_id'
)
as
with deduplicated as (

  select
    product_id,
    lower(trim(product_category_name)) as product_category_name,
    cast(product_name_lenght as int)   as product_name_lenght,
    cast(product_description_lenght as int) as product_description_lenght,
    cast(product_photos_qty as int)    as product_photos_qty,
    cast(product_weight_g as int)      as product_weight_g,
    cast(product_length_cm as int)     as product_length_cm,
    cast(product_height_cm as int)     as product_height_cm,
    cast(product_width_cm as int)      as product_width_cm,
    _source_file,
    _file_modified_at,
    _ingested_at,
    _rescued_data,
    row_number() over (partition by product_id order by _ingested_at desc) as rn

  from stream live.bronze_products
)

select
  product_id,
  product_category_name,
  product_name_lenght,
  product_description_lenght,
  product_photos_qty,
  product_weight_g,
  product_length_cm,
  product_height_cm,
  product_width_cm,

  _source_file,
  _file_modified_at,
  _ingested_at,
  _rescued_data

from deduplicated
where rn = 1;
