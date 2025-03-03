-- ROLE SETLECTION
use role sysadmin;

-- WAREHOUSE'S CREATION

-- ADHOC WAREHOUSE (for all adhoc & development activities)
create warehouse if not exists adhoc
    warehouse_type = 'standard' 
    warehouse_size = 'x-small' 
    auto_resume = true 
    auto_suspend = 60 
    min_cluster_count = 1 
    max_cluster_count = 1 
    scaling_policy = 'standard'
    initially_suspended = true
    enable_query_acceleration = false
    comment = 'this is adhoc warehouse for all adhoc & development activities';

-- USE ADHOC
use warehouse adhoc;

-- INGESTION WAREHOUSE (for data ingestion)
create warehouse if not exists ingestion
     warehouse_type = 'standard' 
     warehouse_size = 'medium' 
     auto_resume = true 
     auto_suspend = 60 
     min_cluster_count = 1 
     max_cluster_count = 1 
     scaling_policy = 'standard'
     enable_query_acceleration = false 
     initially_suspended = true
     comment = 'this is load warehouse for loading all the JSON files';

-- TRANSFORM WAREHOUSE (for ETL workload)
create warehouse if not exists transform
     warehouse_type = 'standard' 
     warehouse_size = 'x-small' 
     auto_resume = true 
     auto_suspend = 60 
     min_cluster_count = 1 
     max_cluster_count = 1 
     scaling_policy = 'standard'
     enable_query_acceleration = false 
     initially_suspended = true
     comment = 'this is ETL warehouse for all loading activity';

-- STREAMLIT WAREHOUSE (for streamlit application usage)
create warehouse if not exists streamlit_wh
     warehouse_type = 'standard' 
     warehouse_size = 'x-small' 
     auto_resume = true
     auto_suspend = 600 
     min_cluster_count = 1 
     max_cluster_count = 1 
     scaling_policy = 'standard'
     enable_query_acceleration = false 
     initially_suspended = true
     comment = 'this is streamlit virtua warehouse';

show warehouses;

-- DATABASE CREATION

create database if not exists aqi;
show databases;

-- SCHEMA'S CREATION

create schema if not exists aqi.stage;
create schema if not exists aqi.clean;
create schema if not exists aqi.consumption;
create schema if not exists aqi.publish;

show schemas;
