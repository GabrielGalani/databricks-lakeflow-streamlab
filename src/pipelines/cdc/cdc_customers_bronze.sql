create or refresh streaming live table olist_lakehouse.bronze.cdc_customers
using delta
comment 'CDC bronze table for customers. Stores all customer change events (insert, update, delete).'
tblproperties (
  'quality' = 'bronze',
  'pipelines.autooptimize.managed' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
as
select
    customer_id,
    customer_unique_id,
    initcap(trim(customer_city)) as customer_city,
    upper(trim(customer_state)) as customer_state,
    cast(customer_zip_code_prefix as int) as customer_zip_code_prefix,

    -- CDC metadata
    'I' AS cdc_operation,
    _metadata.file_path AS _source_file,
    _metadata.file_modification_time AS _file_modified_at,
    current_timestamp() AS cdc_timestamp,
    current_timestamp() AS _ingested_at,
    _rescued_data
from stream read_files(
    "/Volumes/olist_lakehouse/raw/olist/customers",
    format => "csv",
    header => true,
    inferschema => true,
    rescueddatacolumn => "_rescued_data"
)
where customer_id is not null;
