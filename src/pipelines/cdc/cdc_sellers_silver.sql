create or refresh streaming live table ${catalog}.${layer_cdc}.cdc_sellers_stg
tblproperties ('quality' = 'staging')
as 
select 
  * from stream(${catalog}.${layer_cdc}.cdc_sellers_bronze);

create or refresh streaming live table ${catalog}.${layer_cdc}.cdc_sellers_actual_silver
comment 'sellers consolidada - visão atualizada (scd1)'
tblproperties ('quality' = 'silver');

apply changes into ${catalog}.${layer_cdc}.cdc_sellers_actual_silver
from stream(${catalog}.${layer_cdc}.cdc_sellers_stg)
  keys (seller_id)
  apply as delete when operation = 'delete'
  sequence by sequence_number
  columns * except (operation, sequence_number, _source_file, _file_modified_at, cdc_timestamp, _ingested_at, _rescued_data)
stored as scd type 1;

create or refresh streaming live table ${catalog}.${layer_cdc}.cdc_sellers_silver_hist
comment 'sellers histórica - rastreamento de mudanças (scd2)'
tblproperties ('quality' = 'silver');

apply changes into ${catalog}.${layer_cdc}.cdc_sellers_silver_hist
from stream(${catalog}.${layer_cdc}.cdc_sellers_stg)
  keys (seller_id)
  apply as delete when operation = 'delete'
  sequence by sequence_number
  columns * except (operation, sequence_number, _source_file, _file_modified_at, cdc_timestamp, _ingested_at, _rescued_data)
stored as scd type 2;