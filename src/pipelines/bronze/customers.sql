create or refresh streaming live table olist_lakehouse.bronze.bronze_customers
comment 'bronze layer - raw customer data ingested via auto loader'
tblproperties (
  'quality' = 'bronze',
  'pipelines.autooptimize.managed' = 'true',
  'pipelines.autooptimize.zordercols' = 'customer_id'
)
as
select
  *,
  _metadata.file_path as _source_file,
  _metadata.file_modification_time as _file_modified_at,
  current_timestamp() as _ingested_at
from stream read_files(
  "/Volumes/olist_lakehouse/raw/olist/customers",
  format => "csv",
  header => true,
  inferschema => true,
  rescueddatacolumn => "_rescued_data"
);
