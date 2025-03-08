-- SET CONTEXT
use role sysadmin;
use warehouse adhoc;
use schema aqi.consumption;

-- FUNCTION TO FIND PROMINENT POLLUTANT
create or replace function prominent_pollutant(pm25 number, pm10 number, so2 number, no2 number, nh3 number, co number, o3 number)
    returns varchar
    language python
    runtime_version = '3.8'
    handler = 'prominent_pollutant'
as '
def prominent_pollutant(pm25, pm10, so2, no2, nh3, co, o3):

    # HANDLE NONE VALUES BY REPLACING THEM WITH 0
    pm25 = pm25 if pm25 is not None else 0
    pm10 = pm10 if pm10 is not None else 0
    so2 = so2 if so2 is not None else 0
    no2 = no2 if no2 is not None else 0
    nh3 = nh3 if nh3 is not None else 0
    co = co if co is not None else 0
    o3 = o3 if o3 is not None else 0

    # CREATE A DICTIONARY TO MAP VARIABLE NAMES TO THEIR VALUES
    variables = {''PM25'': pm25, ''PM10'': pm10, ''SO2'': so2, ''NO2'': no2, ''NH3'': nh3, ''CO'': co, ''O3'': o3}
    
    # FIND THE VARIABLE WITH THE HIGHEST VALUE
    max_variable = max(variables, key=variables.get)
    
    return max_variable 
';

-- FUNCTION CHECK
select prominent_pollutant(56,70,12,4,17,47,3);


-- AQI CALULATION CRITERIA - Overall AQI is calculated only if data are available for minimum three pollutants 
-- out of which one should necessarily be either PM2.5 or PM10. Else, data are considered insufficient for calculating AQI.

-- FUNCTION TO CHECK THE AQI CALULATION CRITERIA 
create or replace function three_sub_index_criteria(pm25 number, pm10 number, so2 number, no2 number, nh3 number, co number, o3 number)
    returns number(38,0)
    language python
    runtime_version = '3.8'
    handler = 'three_sub_index_criteria'
as '
def three_sub_index_criteria(pm25, pm10, so2, no2, nh3, co, o3  ):
    pm_count = 0
    non_pm_count = 0

    if pm25 is not None and pm25 > 0:
        pm_count = 1
    elif pm10 is not None and pm10 > 0:
        pm_count = 1

    non_pm_count = min(2, sum(p is not None and p != 0 for p in [so2, no2, nh3, co, o3]))

    return pm_count + non_pm_count
';


-- IF CRITERIA IS MET THEN COLLECT POLLUTANT VALUES
create or replace function get_value(input_value varchar)
returns number(38,0)
language sql
as '
    case 
        when input_value is null then 0
        when input_value = ''NA'' then 0
        else to_number(input_value) 
    end
';

