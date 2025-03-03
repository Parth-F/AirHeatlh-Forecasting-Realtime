-- SET CONTEXT
use role sysadmin;
use warehouse adhoc;
use schema aqi.stage;

-- STAGE CREATION (for raw data)
create stage if not exists raw_data
directory = (enable = true)
comment = 'all the air quality raw data will stored in this internal stage';

-- FILE FORMAT CREATION (for JSON files)
create file format if not exists json_ff
    type = 'json'
    compression = 'auto'
    comment = 'this is json file format object';

-- MANUALLY LOADING THE DATA TO THE STAGE - (Snowflake WebUI)

list @raw_data;

-- QUERY LODAED FILE 
select * from @aqi.stage.raw_data 
(file_format => json_ff);

-- QUERY FILE DETAILS
select 
    try_to_timestamp(j.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') as idx_record_ts,
    j.$1 as json_file,
    j.$1:total::int as record_count,
    j.$1:version::text as json_version  
from @aqi.stage.raw_data
(file_format => json_ff) j;

-- QUERY FILE METADATA
select 
    try_to_timestamp(j.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') as idx_record_ts,
    j.$1 as json_file,
    j.$1:total::int as record_count,
    j.$1:version::text as json_version,
    
    -- METADATA 
    metadata$filename as _stg_file_name,
    metadata$file_last_modified as _stg_file_load_ts,
    metadata$file_content_key as _stg_file_md5,
    current_timestamp() as _copy_data_ts
from @aqi.stage.raw_data
(file_format => json_ff) j;


-- CREAT RAW AIR QUALITY TABLE
create or replace transient table raw_aqi (
    id int primary key autoincrement,
    idx_record_ts timestamp not null,
    json_data variant not null,
    record_count number not null default 0,
    json_version text not null,
    
    -- USED AS AUDIT COLUMNS FOR DEBUGGING
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp()
);

-- COPY DATA INTO RAW TABLE MANUALLY USING COPY COMMAND
-- copy into raw_aqi (idx_record_ts,json_data,record_count,json_version,_stg_file_name,_stg_file_load_ts,_stg_file_md5,_copy_data_ts) from 
--     (
--         select 
--             try_to_timestamp(j.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') as idx_record_ts,
--             j.$1 as json_file,
--             j.$1:total::int as record_count,
--             j.$1:version::text as json_version,
--             metadata$filename as _stg_file_name,
--             metadata$file_last_modified as _stg_file_load_ts,
--             metadata$file_content_key as _stg_file_md5,
--             current_timestamp() as _copy_data_ts   
--        from @aqi.stage.raw_data as j
--     )
-- file_format = (format_name = 'aqi.stage.json_ff');


-- CREATING A TASK TO AUTOMATE THE PROCESS OF DATA INGESTION INTO THE RAW_AQI TABLE
create or replace task copy_aqi_data
    warehouse = ingestion
    schedule = 'USING CRON 0 * * * * Asia/Kolkata'
as
copy into raw_aqi (idx_record_ts,json_data,record_count,json_version,_stg_file_name,_stg_file_load_ts,_stg_file_md5,_copy_data_ts) from 
    (
        select 
            try_to_timestamp(j.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') as idx_record_ts,
            j.$1 as json_file,
            j.$1:total::int as record_count,
            j.$1:version::text as json_version,
            metadata$filename as _stg_file_name,
            metadata$file_last_modified as _stg_file_load_ts,
            metadata$file_content_key as _stg_file_md5,
            current_timestamp() as _copy_data_ts   
       from @aqi.stage.raw_data as j
    )
    file_format = (format_name = 'aqi.stage.json_ff')
on_error = abort_statement; 

-- GRANTING PRIVILAGES TO SYSADMIN TO RESUME THE COPY TASK

use role accountadmin;
grant execute task, execute managed task on account to role sysadmin;
use role sysadmin;

-- RESUME THE COPY TASK
alter task aqi.stage.copy_air_quality_data resume;

-- QUERY DATA FROM TABLE
select count(*) from raw_aqi   
limit 10;

-- QUERY DATA BASED BY FILE RANK BASED ON LOAD TIME (usefull when handeling duplicate files)
select
    idx_record_ts,
    record_count,
    json_version,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts,
    row_number() over 
        (
            partition by idx_record_ts 
            order by _stg_file_load_ts desc
        ) 
        as latest_file_rank
from raw_aqi
order by idx_record_ts desc
limit 10;
        
