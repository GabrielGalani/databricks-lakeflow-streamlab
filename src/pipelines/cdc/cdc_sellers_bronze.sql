create or refresh streaming live table ${catalog}.${layer_cdc}.cdc_sellers_bronze
comment 'bronze cdc table for sellers using auto loader and logical cdc pattern'
tblproperties (
  'quality' = 'bronze',
  'pipelines.autooptimize.managed' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
as
select
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    
    -- cdc standard columns
    CAST(sequence_number AS BIGINT) AS sequence_number,
    UPPER(TRIM(operation)) AS operation,
    _metadata.file_path as _source_file,
    _metadata.file_modification_time as _file_modified_at,
    current_timestamp() as cdc_timestamp,
    current_timestamp() as _ingested_at,
    _rescued_data
from stream read_files(
    "/Volumes/olist_lakehouse/raw/olist/cdc/sellers",
    format => "csv",
    header => true,
    inferschema => true,
    rescueddatacolumn => "_rescued_data"
)
where seller_id is not null;
