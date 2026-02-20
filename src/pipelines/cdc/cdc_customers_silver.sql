create or refresh streaming live table olist_lakehouse.silver.cdc_customers_stg
tblproperties ('quality' = 'staging')
as 
select * from stream(olist_lakehouse.bronze.cdc_customers_bronze);

create or refresh streaming live table olist_lakehouse.silver.cdc_customers_silver_actual
comment 'tabela de clientes consolidada (scd1)'
tblproperties ('quality' = 'silver');

apply changes into olist_lakehouse.silver.cdc_customers_silver_actual
from stream(olist_lakehouse.silver.cdc_customers_stg)
  keys (customer_id)
  apply as delete when operation = 'delete'
  sequence by sequence_number
  columns * except (operation, sequence_number, _source_file, _file_modified_at, cdc_timestamp, _ingested_at, _rescued_data)
stored as scd type 1;

create or refresh streaming live table olist_lakehouse.silver.cdc_customers_silver_hist
comment 'tabela de clientes histórica (scd2)'
tblproperties ('quality' = 'silver');

apply changes into olist_lakehouse.silver.cdc_customers_silver_hist
from stream(olist_lakehouse.silver.cdc_customers_stg)
  keys (customer_id)
  apply as delete when operation = 'delete'
  sequence by sequence_number
  columns * except (operation, sequence_number, _source_file, _file_modified_at, cdc_timestamp, _ingested_at, _rescued_data)
stored as scd type 2;