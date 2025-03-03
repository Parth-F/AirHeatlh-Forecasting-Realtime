-- SET CONTEXT
use role sysadmin;
use warehouse adhoc;
use schema aqi.consumption;

-- QUERY DATA BY CITY USING FACT & DIMENSION TABLE
-- part-1 Query Data 
select 
    d.measurement_time,
    l.country,
    l.state,
    l.city,
    avg(pm10_avg) as pm10_avg,
    avg(pm25_avg) as pm25_avg,
    avg(so2_avg) as so2_avg,
    avg(no2_avg) as no2_avg,
    avg(nh3_avg) as nh3_avg,
    avg(co_avg) as co_avg,
    avg(o3_avg) as o3_avg
from 
    aqi_fact a
    join location_dim l on a.location_fk = l.location_pk
    join date_dim d on a.date_fk = d.date_pk
group by 
    1,2,3,4;

    
-- part-2 Query Data & calculate Prominent Pollutant and AQI
with city_level_data as (
select 
    d.measurement_time,
    l.country,
    l.state,
    l.city,
    avg(pm10_avg) as pm10_avg,
    avg(pm25_avg) as pm25_avg,
    avg(so2_avg) as so2_avg,
    avg(no2_avg) as no2_avg,
    avg(nh3_avg) as nh3_avg,
    avg(co_avg) as co_avg,
    avg(o3_avg) as o3_avg
from 
    aqi_fact a
    join date_dim d on a.date_fk = d.date_pk
    join location_dim l on a.location_fk = l.location_pk
group by 
    1,2,3,4
)
select 
    *,
    prominent_pollutant( pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg ) as prominent_pollutant,
        case
            when three_sub_index_criteria( pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg) > 2 
            then greatest ( pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg)
            else 0
        end
        as aqi
from 
    city_level_data
order by 
    country, state, city, measurement_time;



-- CREATING TABLE BY TAKING AGGREGATE OF ALL STATION'S FOR - AQI CITY PER HOUR
create or replace dynamic table aqi_city_per_hr
    target_lag='30 min'
    warehouse=transform
as 
with city_level_data as (
select 
    d.measurement_time,
    l.country,
    l.state,
    l.city,
    avg(pm25_avg) as pm25_avg,
    avg(pm10_avg) as pm10_avg,
    avg(so2_avg) as so2_avg,
    avg(no2_avg) as no2_avg,
    avg(nh3_avg) as nh3_avg,
    avg(co_avg) as co_avg,
    avg(o3_avg) as o3_avg
from 
    aqi_fact f
    join date_dim d on f.date_fk = d.date_pk
    join location_dim l on f.location_fk = l.location_pk
group by 
    1,2,3,4
)
select 
    *,
    prominent_pollutant(pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg) as prominent_pollutant,
        case
            when three_sub_index_criteria( pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg) > 2 
            then greatest (pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg)
            else 0
        end
        as aqi
from 
    city_level_data;

    

-- QUERYING AQI CITY PER HOUR
select 
    * 
from aqi_city_per_hr
order by
    state, city, measurement_time
limit 100;


-- QUERYING AQI CITY PER HOUR - FOR SPECIFIC TIMESTAMP AND CITY (Should only get 1 result per City)
select 
    * 
from aqi_city_per_hr
where 
    city = 'Amaravati' and 
    measurement_time ='2024-03-01 00:00:00.000'
limit 100;


-- CREATING TABLE BY TAKING AGGREGATE OF ALL STATION'S FOR - AQI CITY PER DAY
create or replace dynamic table aqi_city_per_day
    target_lag='30 min'
    warehouse=transform
as 
with city_day_level_data as (
select 
    date(measurement_time) as measurement_date,
    country as country,
    state as state,
    city as city,
    round(avg(pm10_avg)) as pm10_avg,
    round(avg(pm25_avg)) as pm25_avg,
    round(avg(so2_avg)) as so2_avg,
    round(avg(no2_avg)) as no2_avg,
    round(avg(nh3_avg)) as nh3_avg,
    round(avg(co_avg)) as co_avg,
    round(avg(o3_avg)) as o3_avg
from 
    aqi_city_per_hr
group by 
    1,2,3,4
)
select 
    *,
    prominent_pollutant(pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg) as prominent_pollutant,
        case
            when three_sub_index_criteria(pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg) > 2 
            then greatest (pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)
            else 0
        end
        as aqi
from 
    city_day_level_data;

