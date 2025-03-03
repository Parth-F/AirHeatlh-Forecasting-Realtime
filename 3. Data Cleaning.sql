-- SET CONTEXT
use role sysadmin;
use warehouse adhoc;
use schema aqi.clean;

-- QUERY TABLLE RAW_AQI
select * from aqi.stage.raw_aqi order by id;

-- QUERY WHERE TIMESTAMP NOT NULL
select
    id,
    idx_record_ts,
    json_data,
    record_count,
    json_version,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
from
    aqi.stage.raw_aqi
where
    idx_record_ts is not null
order by id;

-- HANDLING DUPLICATE RECORDS + FLATTENING THE DATA
with air_quality_with_rank as 
    (
        select 
            id,
            idx_record_ts,
            json_data,
            record_count,
            json_version,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5 ,
            _copy_data_ts,
            row_number() over 
              (
                partition by idx_record_ts 
                order by _stg_file_load_ts desc
              ) 
            as latest_file_rank
        from aqi.stage.raw_aqi
        where idx_record_ts is not null 
    ),
unique_air_quality_data as 
    (
        select 
            * 
        from 
            air_quality_with_rank 
        where latest_file_rank = 1
    )
select 
    id,
    idx_record_ts,
    hrly_rec.value:country::text as country,
    hrly_rec.value:state::text as state,
    hrly_rec.value:city::text as city,
    hrly_rec.value:station::text as station,
    hrly_rec.value:latitude::number(12,7) as latitude,
    hrly_rec.value:longitude::number(12,7) as longitude,
    hrly_rec.value:pollutant_id::text as pollutant_id,
    hrly_rec.value:pollutant_max::text as pollutant_max,
    hrly_rec.value:pollutant_min::text as pollutant_min,
    hrly_rec.value:pollutant_avg::text as pollutant_avg,

    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
from 
unique_air_quality_data ,
lateral flatten (input => json_data:records) hrly_rec
order by id;


-- CREATE DYNAMIC TABLE USING ABOVE CTE's
create or replace dynamic table cleaned_aqi
    target_lag='downstream'
    warehouse=transform
as
with air_quality_with_rank as 
    (
        select 
            id,
            idx_record_ts,
            json_data,
            record_count,
            json_version,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5 ,
            _copy_data_ts,
            row_number() over 
              (
                partition by idx_record_ts 
                order by _stg_file_load_ts desc
              ) 
            as latest_file_rank
        from aqi.stage.raw_aqi
        where idx_record_ts is not null 
    ),
unique_air_quality_data as 
    (
        select 
            * 
        from 
            air_quality_with_rank 
        where latest_file_rank = 1
    )
select 
    id,
    idx_record_ts,
    hrly_rec.value:country::text as country,
    hrly_rec.value:state::text as state,
    hrly_rec.value:city::text as city,
    hrly_rec.value:station::text as station,
    hrly_rec.value:latitude::number(12,7) as latitude,
    hrly_rec.value:longitude::number(12,7) as longitude,
    hrly_rec.value:pollutant_id::text as pollutant_id,
    hrly_rec.value:pollutant_max::text as pollutant_max,
    hrly_rec.value:pollutant_min::text as pollutant_min,
    hrly_rec.value:pollutant_avg::text as pollutant_avg,

    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
from 
unique_air_quality_data ,
lateral flatten (input => json_data:records) hrly_rec;

-- VIEW CLEAN AQI TABLE
select * from cleaned_aqi 
order by id
limit 10;
