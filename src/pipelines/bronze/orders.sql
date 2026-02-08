CREATE OR REFRESH STREAMING LIVE TABLE bronze_orders
COMMENT 'Bronze layer - raw order data ingested via Auto Loader'
TBLPROPERTIES (
  'quality' = 'bronze',
  'pipelines.autoOptimize.managed' = 'true',
  'pipelines.autoOptimize.zOrderCols' = 'order_id'
)
AS
SELECT
  *,
  _metadata.file_path AS _source_file,
  _metadata.file_modification_time AS _file_modified_at,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  "/Volumes/olist_lakehouse/raw/olist/orders/",
  format => "csv",
  header => true,
  inferSchema => true,
  rescuedDataColumn => "_rescued_data"
);
