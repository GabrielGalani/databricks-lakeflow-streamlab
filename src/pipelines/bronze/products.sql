create or refresh streaming live table bronze.bronze_products
comment 'bronze layer - raw products data ingested via auto loader'
tblproperties (
  'quality' = 'bronze',
  'pipelines.autooptimize.managed' = 'true',
  'pipelines.autooptimize.zordercols' = 'product_id'
)
as
select
  *,
  _metadata.file_path as _source_file,
  _metadata.file_modification_time as _file_modified_at,
  current_timestamp() as _ingested_at
from stream read_files(
  "/volumes/olist_lakehouse/raw/olist/products/",
  format => "csv",
  header => true,
  inferschema => true,
  rescueddatacolumn => "_rescued_data"
);
