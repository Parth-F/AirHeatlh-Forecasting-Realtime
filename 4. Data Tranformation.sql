-- SET CONTEXT
use role sysadmin;
use warehouse adhoc;
use schema aqi.clean;

-- QUERY DATA FROM SPECIFIC LOCATION
select
    hour(idx_record_ts) as measurement_hours,
    *
from
    cleaned_aqi
where
    country = 'India' and 
    state = 'Delhi' and
    station = 'Mundka, Delhi - DPCC'
order by 
    idx_record_ts, id;

-- TRANSPOSE THE TABLE

-- The dataset includes 7 pollutant's with their MIN, MAX, and AVG values.  
-- Only the AVG values are needed, so the data must be transposed from rows to columns.  
-- Combine all AVG values into a single entry instead of multiple entries for each pollutant.  

select 
        idx_record_ts,
        country,
        state,
        city,
        station,
        latitude,
        longitude,
        max(case when pollutant_id = 'PM2.5' then pollutant_avg end) as pm25_avg,
        max(case when pollutant_id = 'PM10' then pollutant_avg end) as pm10_avg,
        max(case when pollutant_id = 'SO2' then pollutant_avg end) as so2_avg,
        max(case when pollutant_id = 'NO2' then pollutant_avg end) as no2_avg,
        max(case when pollutant_id = 'NH3' then pollutant_avg end) as nh3_avg,
        max(case when pollutant_id = 'CO' then pollutant_avg end) as co_avg,
        max(case when pollutant_id = 'OZONE' then pollutant_avg end) as o3_avg
    from
        cleaned_aqi
    where
        country = 'India' and
        state = 'Karnataka' and
        station = 'Silk Board, Bengaluru - KSPCB' and
        idx_record_ts = '2024-03-01 11:00:00.000'
    group by
        idx_record_ts, country, state, city, station, latitude, longitude
    order by country, state, city, station;


-- CREATE A TEMPORARY TABLE WITH TRANSPOSE DATA
create or replace temp table air_quality_tmp as
select 
        idx_record_ts,
        country,
        state,
        city,
        station,
        latitude,
        longitude,
        max(case when pollutant_id = 'PM2.5' then pollutant_avg end) as pm25_avg,
        max(case when pollutant_id = 'PM10' then pollutant_avg end) as pm10_avg,
        max(case when pollutant_id = 'SO2' then pollutant_avg end) as so2_avg,
        max(case when pollutant_id = 'NO2' then pollutant_avg end) as no2_avg,
        max(case when pollutant_id = 'NH3' then pollutant_avg end) as nh3_avg,
        max(case when pollutant_id = 'CO' then pollutant_avg end) as co_avg,
        max(case when pollutant_id = 'OZONE' then pollutant_avg end) as o3_avg
    from
        cleaned_aqi
    group by
        idx_record_ts, country, state, city, station, latitude, longitude
    order by country, state, city, station;


-- TESTING TEMPORARY TABLE
select
    HOUR(idx_record_ts) as measurment_hours,
    *
from 
    air_quality_tmp
where 
    country = 'India' and
    state = 'Delhi' and 
    station = 'IGI Airport (T3), Delhi - IMD';

-- We can have some NA & NULL we need to Handel, so we need to handle them.
-- also, rounding the the values to nearest integer.

-- HANDLING NA / NULL VALUES & ROUNDING THE VALUES 
select 
        idx_record_ts,
        country,
        -- state, 
        replace(state,'_',' ') as state,
        city,
        station,
        latitude,
        longitude,
        case 
            when pm10_avg = 'NA' then 0 
            when pm10_avg is null then 0 
            else round(pm10_avg)
        end as pm10_avg,
        case 
            when pm25_avg = 'NA' then 0 
            when pm25_avg is null then 0 
            else round(pm25_avg)
        end as pm25_avg,
        case 
            when so2_avg = 'NA' then 0 
            when so2_avg is null then 0 
            else round(so2_avg)
        end as so2_avg,
         case 
            when nh3_avg = 'NA' then 0 
            when nh3_avg is null then 0 
            else round(nh3_avg)
        end as nh3_avg,
        case 
            when no2_avg = 'NA' then 0 
            when no2_avg is null then 0 
            else round(no2_avg)
        end as no2_avg,
         case 
            when co_avg = 'NA' then 0 
            when co_avg is null then 0 
            else round(co_avg)
        end as co_avg,
         case 
            when o3_avg = 'NA' then 0 
            when o3_avg is null then 0 
            else round(o3_avg)
        end as o3_avg,
    from air_quality_tmp;


-- CREATING A TRANSFORMED TABLE USING ABOVE TESTED CTE's
create or replace dynamic table transformed_aqi
    target_lag='30 min'
    warehouse=transform
as
with combine_pollutant as (
    select 
        idx_record_ts,
        country,
        state,
        city,
        station,
        latitude,
        longitude,
        max(case when pollutant_id = 'PM2.5' then pollutant_avg end) as pm25_avg,
        max(case when pollutant_id = 'PM10' then pollutant_avg end) as pm10_avg,
        max(case when pollutant_id = 'SO2' then pollutant_avg end) as so2_avg,
        max(case when pollutant_id = 'NO2' then pollutant_avg end) as no2_avg,
        max(case when pollutant_id = 'NH3' then pollutant_avg end) as nh3_avg,
        max(case when pollutant_id = 'CO' then pollutant_avg end) as co_avg,
        max(case when pollutant_id = 'OZONE' then pollutant_avg end) as o3_avg
    from 
        cleaned_aqi
    group by 
        idx_record_ts, country, state, city, station, latitude, longitude
        order by country, state, city, station
),
replace_na as (
    select 
        idx_record_ts,
        country,
        replace(state,'_',' ') as state,
        city,
        station,
        latitude,
        longitude,
        case 
            when pm10_avg = 'NA' then 0 
            when pm10_avg is null then 0 
            else round(pm10_avg)
        end as pm10_avg,
        case 
            when pm25_avg = 'NA' then 0 
            when pm25_avg is null then 0 
            else round(pm25_avg)
        end as pm25_avg,
        case 
            when so2_avg = 'NA' then 0 
            when so2_avg is null then 0 
            else round(so2_avg)
        end as so2_avg,
         case 
            when nh3_avg = 'NA' then 0 
            when nh3_avg is null then 0 
            else round(nh3_avg)
        end as nh3_avg,
        case 
            when no2_avg = 'NA' then 0 
            when no2_avg is null then 0 
            else round(no2_avg)
        end as no2_avg,
         case 
            when co_avg = 'NA' then 0 
            when co_avg is null then 0 
            else round(co_avg)
        end as co_avg,
         case 
            when o3_avg = 'NA' then 0 
            when o3_avg is null then 0 
            else round(o3_avg)
        end as o3_avg,
    from combine_pollutant
)
select *,
from replace_na;


-- VIEWING TABLE
select * from transformed_aqi limit 10;
