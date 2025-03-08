-- SET CONTEXT
use role sysadmin;
use warehouse adhoc;
use schema aqi.consumption;

-- STORE THE DATA INTO FACT AND DIMENSION TABLE

-- To Store the data of the air quality index, we are normalizing the data into a fact and dimension table.
-- as LOCATION, DATE, are REDUNDANT data, as DATES and LOCATIONS will be same for multiple records.
-- that's why we are creating a dimension table for LOCATION and DATE and a fact table for the AQI data.

-- DATE DIMENSTION TABLE
-- part-1 query to get the unique date values
select 
        idx_record_ts as measurement_time,
        year(idx_record_ts) as aqi_year,
        month(idx_record_ts) as aqi_month,
        quarter(idx_record_ts) as aqi_quarter,
        day(idx_record_ts) aqi_day,
        hour(idx_record_ts) aqi_hour,
    from 
        aqi.clean.transformed_aqi
        group by 1,2,3,4,5,6;

        
-- part-2 hash key for date dimension table
with hr_data as (
select 
        idx_record_ts as measurement_time,
        year(idx_record_ts) as aqi_year,
        month(idx_record_ts) as aqi_month,
        quarter(idx_record_ts) as aqi_quarter,
        day(idx_record_ts) aqi_day,
        hour(idx_record_ts)+1 aqi_hour,
    from 
        aqi.clean.transformed_aqi
        group by 1,2,3,4,5,6
)
select 
    hash(measurement_time) as date_id,
    *
from hr_data
order by aqi_year,aqi_month,aqi_day,aqi_hour;


-- DATE DIMENSION TABLE CREATION BY CTE
create or replace dynamic table date_dim
        target_lag='DOWNSTREAM'
        warehouse=transform
    as
    with hr_data as (
    select 
            idx_record_ts as measurement_time,
            year(idx_record_ts) as aqi_year,
            month(idx_record_ts) as aqi_month,
            quarter(idx_record_ts) as aqi_quarter,
            day(idx_record_ts) aqi_day,
            hour(idx_record_ts)+1 aqi_hour,
        from 
            aqi.clean.transformed_aqi
            group by 1,2,3,4,5,6
    )
    select 
        hash(measurement_time) as date_pk,
        *
    from hr_data
    order by aqi_year,aqi_month,aqi_day,aqi_hour;
    
select * from date_dim;

-- LOCATION DIMENSTION TABLE
-- part-1 query to get the unique LOCATION values
select 
    LATITUDE,
    LONGITUDE,
    COUNTRY,
    STATE,
    CITY,
    STATION,
from 
    aqi.clean.transformed_aqi
    group by 1,2,3,4,5,6;

  
-- part-2 hash key for LOCATION dimension table
with unique_loc_data as (
select 
    LATITUDE,
    LONGITUDE,
    COUNTRY,
    STATE,
    CITY,
    STATION,
from 
    aqi.clean.transformed_aqi
    group by 1,2,3,4,5,6
)
select 
    hash(LATITUDE,LONGITUDE) as LOCATION_PK,
    *
from unique_loc_data
order by 
    country, STATE, city, station;

show tables;


-- LOCATION DIMENSION TABLE CREATION BY CTE
create or replace dynamic table LOCATION_DIM
        target_lag='DOWNSTREAM'
        warehouse=transform
    as
    with unique_loc_data as (
    select 
        latitude,
        longitude,
        country,
        state,
        city,
        station,
    from 
        aqi.clean.transformed_aqi
        group by 1,2,3,4,5,6
    )
    select 
        hash(LATITUDE,LONGITUDE) as LOCATION_PK,
        *
    from unique_loc_data
    order by 
        country, STATE, city, station;

        
-- AQI FACT TABLE
-- part-1 query to get the unique AQI values
select 
        idx_record_ts,
        year(idx_record_ts) as aqi_year,
        month(idx_record_ts) as aqi_month,
        quarter(idx_record_ts) as aqi_quarter,
        day(idx_record_ts) aqi_day,
        hour(idx_record_ts) aqi_hour,
        country,
        state,
        city,
        station,
        latitude,
        longitude,
        pm10_avg,
        pm25_avg,
        so2_avg,
        no2_avg,
        nh3_avg,
        co_avg,
        o3_avg,
        prominent_pollutant(PM25_AVG, PM10_AVG, SO2_AVG, NO2_AVG, NH3_AVG, CO_AVG, O3_AVG) as prominent_pollutant,
        case
          when three_sub_index_criteria(PM25_AVG, PM10_AVG, SO2_AVG, NO2_AVG, NH3_AVG, CO_AVG, O3_AVG ) > 2 
          then greatest ( get_value(PM25_AVG), get_value(PM10_AVG), get_value(SO2_AVG), get_value(NO2_AVG), get_value(NH3_AVG), get_value(CO_AVG), get_value(O3_AVG))
          else 0
        end
        as aqi
    from aqi.clean.transformed_aqi
    limit 100;
    

-- part-2 genrate hash key for one of the unique AQI values
select 
        hash(idx_record_ts) as date_fk,
        hash(latitude, longitude) as LOCATION_FK,
        pm10_avg,
        pm25_avg,
        so2_avg,
        no2_avg,
        nh3_avg,
        co_avg,
        o3_avg,
        prominent_pollutant( pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg) as prominent_pollutant,
        case
          when three_sub_index_criteria( pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg) > 2 
          then greatest ( get_value(pm25_avg), get_value(pm10_avg), get_value(so2_avg), get_value(no2_avg), get_value(nh3_avg), get_value(co_avg), get_value(o3_avg))
          else 0
        end
        as aqi
    from aqi.clean.transformed_aqi
    where 
        city = 'Latur' and 
        station =  'Sawe Wadi, Latur - MPCB' and 
        IDX_RECORD_TS = '2024-03-01 19:00:00.000';

        

-- part-3 checking if the hash key is unique and working to display the data accuretly
select * from date_dim where date_pk = -6395434501082770626;
select * from LOCATION_DIM where LOCATION_PK = 3464970948614270270;



-- AQI FACT TABLE CREATION
create or replace dynamic table aqi_fact
        target_lag='30 min'
        warehouse=transform
    as
    select 
            hash(idx_record_ts,latitude,longitude) aqi_pk,
            hash(idx_record_ts) as date_fk,
            hash(latitude,longitude) as LOCATION_FK,
            pm10_avg,
            pm25_avg,
            so2_avg,
            no2_avg,
            nh3_avg,
            co_avg,
            o3_avg,
            prominent_pollutant(pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)as prominent_pollutant,
            case
            when three_sub_index_criteria(pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg) > 2 
            then greatest (get_value(pm25_avg),get_value(pm10_avg),get_value(so2_avg),get_value(no2_avg),get_value(nh3_avg),get_value(co_avg),get_value(o3_avg))
            else 0
            end
        as aqi
        from aqi.clean.transformed_aqi;
