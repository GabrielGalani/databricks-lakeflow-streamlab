CREATE OR REFRESH STREAMING LIVE TABLE bronze_order_reviews
COMMENT 'Bronze layer - raw order review data ingested via Auto Loader'
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
  "/Volumes/olist_lakehouse/raw/olist/order_reviews/",
  format => "csv",
  header => true,
  inferSchema => true,
  rescuedDataColumn => "_rescued_data"
);
