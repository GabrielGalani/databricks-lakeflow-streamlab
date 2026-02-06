USE CATALOG olist_lakehouse;

CREATE VOLUME IF NOT EXISTS bronze.raw_volume
COMMENT 'Raw data landing zone for Auto Loader';

CREATE VOLUME IF NOT EXISTS cdc.cdc_volume
COMMENT 'CDC incoming data';
