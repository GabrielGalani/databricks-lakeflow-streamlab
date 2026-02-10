USE CATALOG olist_lakehouse;

CREATE VOLUME IF NOT EXISTS cdc.cdc_volume
COMMENT 'CDC incoming data';

CREATE VOLUME IF NOT EXISTS raw.olist
COMMENT 'CDC incoming data';