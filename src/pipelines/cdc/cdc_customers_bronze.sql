create or refresh streaming live table ${catalog}.${layer_cdc}.cdc_customers_bronze
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
    customer_city,
    customer_state,
    customer_zip_code_prefix,
    customer_email,
    customer_name,
    customer_phone,

    -- CDC metadata
    CAST(sequence_number AS BIGINT) AS sequence_number,
    UPPER(TRIM(operation)) AS operation,
    _metadata.file_path AS _source_file,
    _metadata.file_modification_time AS _file_modified_at,
    current_timestamp() AS cdc_timestamp,
    current_timestamp() AS _ingested_at,
    _rescued_data
from stream read_files(
    "/Volumes/olist_lakehouse/raw/olist/cdc/customers",
    format => "csv",
    header => true,
    inferschema => true,
    rescueddatacolumn => "_rescued_data"
)
where customer_id is not null;
