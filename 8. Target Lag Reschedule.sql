-- SET CONTEXT
use role sysadmin;
use warehouse adhoc;


-- RESCHEDULE 30 MINUTES DATA LOADING TIME TO RAW STAGE 
alter task aqi.stage.copy_aqi_data suspend;

alter task aqi.stage.copy_aqi_data 
set schedule = '30 MINUTES';

alter task aqi.stage.copy_aqi_data resume;


-- CHANGE THE ALL THE TARGET_LAG TO DOWNSTREAM, AND SET TARGET_LAG = '30 MINUTES'
alter dynamic table aqi.clean.cleaned_aqi
set target_lag = 'DOWNSTREAM';

alter dynamic table aqi.clean.transformed_aqi
set target_lag = 'DOWNSTREAM';

alter dynamic table aqi.consumption.date_dim 
set target_lag = 'DOWNSTREAM';

alter dynamic table aqi.consumption.location_dim 
set target_lag = 'DOWNSTREAM';

alter dynamic table aqi.consumption.aqi_fact 
set target_lag = 'DOWNSTREAM';

alter dynamic table aqi.consumption.aqi_city_per_hr 
set target_lag = 'DOWNSTREAM';

alter dynamic table aqi.consumption.aqi_city_per_day 
set target_lag = '30 MINUTES';



