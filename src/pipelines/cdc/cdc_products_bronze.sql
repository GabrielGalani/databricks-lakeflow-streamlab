create or refresh streaming live table cdc_products_bronze
comment 'bronze cdc table for products using auto loader and logical cdc flags'
tblproperties (
  'quality' = 'bronze',
  'pipelines.autooptimize.managed' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
as
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

    -- cdc control columns
    'I' AS cdc_operation,
    _metadata.file_path AS _source_file,
    _metadata.file_modification_time AS _file_modified_at,
    current_timestamp() AS cdc_timestamp,
    current_timestamp() AS _ingested_at,
    _rescued_data
from stream read_files(
    "/Volumes/olist_lakehouse/raw/olist/products",
    format => "csv",
    header => true,
    inferschema => true,
    rescueddatacolumn => "_rescued_data"
)
where product_id is not null;
