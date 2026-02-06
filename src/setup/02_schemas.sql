USE CATALOG olist_lakehouse;

CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Bronze layer - raw ingested data';

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Silver layer - cleansed and conformed data';

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Gold layer - business aggregates';

CREATE SCHEMA IF NOT EXISTS cdc
COMMENT 'CDC processing layer';

CREATE SCHEMA IF NOT EXISTS utils
COMMENT 'Utility objects and functions';
