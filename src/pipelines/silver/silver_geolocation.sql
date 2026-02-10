create or refresh materialized view olist_lakehouse.silver.silver_geolocation
comment 'cleansed and deduplicated geolocation reference data with averaged coordinates'
tblproperties (
    'quality' = 'silver'
)
as
with deduplicated as (
    select
        cast(geolocation_zip_code_prefix as int) as zip_code_prefix,
        initcap(trim(geolocation_city)) as city,
        upper(trim(geolocation_state)) as state,
        avg(cast(geolocation_lat as double)) as latitude,
        avg(cast(geolocation_lng as double)) as longitude,
        count(*) as sample_count
    from olist_lakehouse.bronze.bronze_geolocation
    where 
      geolocation_zip_code_prefix is not null
      and geolocation_lat is not null
      and geolocation_lng is not null
      and _rescued_data is null
    group by
        cast(geolocation_zip_code_prefix as int), initcap(trim(geolocation_city)), upper(trim(geolocation_state))
)
select
    zip_code_prefix,
    city,
    state,
    round(latitude, 6) as latitude,
    round(longitude, 6) as longitude,
    sample_count,
    case state
        when 'sp' then 'southeast'
        when 'rj' then 'southeast'
        when 'mg' then 'southeast'
        when 'es' then 'southeast'
        when 'pr' then 'south'
        when 'sc' then 'south'
        when 'rs' then 'south'
        when 'ms' then 'midwest'
        when 'mt' then 'midwest'
        when 'go' then 'midwest'
        when 'df' then 'midwest'
        when 'ba' then 'northeast'
        when 'se' then 'northeast'
        when 'al' then 'northeast'
        when 'pe' then 'northeast'
        when 'pb' then 'northeast'
        when 'rn' then 'northeast'
        when 'ce' then 'northeast'
        when 'pi' then 'northeast'
        when 'ma' then 'northeast'
        when 'pa' then 'north'
        when 'am' then 'north'
        when 'ap' then 'north'
        when 'rr' then 'north'
        when 'ac' then 'north'
        when 'ro' then 'north'
        when 'to' then 'north'
        else 'unknown'
    end as region,
    current_timestamp() as _processed_at
from deduplicated;