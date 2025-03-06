-- SET CONTEXT
use role sysadmin;
use warehouse adhoc;
use schema aqi.stage;


-- Marketplat data integration
-- lets see how we can query the marketplace data.
select 
    'India' as country,
    'New Delhi' as state,
    'New Delhi' as city,
    date_valid_std as measurement_dt,
    avg_temperature_air_2m_f as temperature_in_f
from 
    global_weather__climate_data_for_bi.standard_tile.history_day
where 
    country = 'IN' and
    date_valid_std > '2025-01-01';

    
-- lets try to create a dynamic table from marketplace view.
create or replace dynamic table weather_data
    target_lag='1 hour'
    warehouse = transform
as
select 
    'India' as country,
    'New Delhi' as state,
    'New Delhi' as city,
    date_valid_std as measurement_dt,
    avg_temperature_air_2m_f as temperature_in_f
from 
    global_weather__climate_data_for_bi.standard_tile.history_day
where 
    country = 'IN' and
    date_valid_std > '2025-02-11';






-- create a table with historical data load
create or replace table aqi.consumption.weather_data as 
select 
    'India' as country,
    'Delhi' as state,
    'Delhi' as city,
    DATE_VALID_STD as measurement_dt,
    AVG_TEMPERATURE_AIR_2M_F as temperature_in_f
from 
    global_weather__climate_data_for_bi.standard_tile.history_day
where 
    country = 'IN' and
    DATE_VALID_STD between '2025-01-01' and current_date();

-- check the data
select * from aqi.consumption.weather_data order by measurement_dt;


-- create a task that runs everyday mid night at 1AM
create or replace task refresh_weather_data_task
    warehouse = ingestion
    schedule = 'USING CRON 55 23 * * * Asia/Kolkata'
as
insert into aqi.consumption.weather_data select  
    'India' as country,
    'Delhi' as state,
    'Delhi' as city,
    DATE_VALID_STD as measurement_dt,
    AVG_TEMPERATURE_AIR_2M_F as temperature_in_f
from 
    global_weather__climate_data_for_bi.standard_tile.history_day
where 
    country = 'IN' and
    DATE_VALID_STD  =  current_date();

    

-- aggregated data
create or replace dynamic table agg_delhi_fact_day_level
    target_lag='60 min'
    warehouse=transform
as 
    select 
        a.measurement_date,
        a.country,
        a.state,
        a.city,
        a.pm10_avg,
        a.pm25_avg,
        a.so2_avg,
        a.no2_avg,
        a.nh3_avg,
        a.co_avg,
        a.o3_avg,
        t.temperature_in_f,
        a.PROMINENT_POLLUTANT,
        a.AQI,
    from 
        AGG_CITY_FACT_DAY_LEVEL a join 
        weather_data t on 
        a.measurement_date = t.measurement_dt and 
        a.country = t.country and 
        a.state = t.state and 
        a.city = t.city 
        ;

select * from AGG_CITY_FACT_DAY_LEVEL;
select * from agg_delhi_fact_day_level;

    