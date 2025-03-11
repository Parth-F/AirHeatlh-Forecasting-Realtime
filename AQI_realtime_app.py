# import python packages
import streamlit as st
import pandas as pd
from decimal import decimal

import snowflake.connector
from snowflake.snowpark import session
from snowflake.snowpark.context import get_active_session


# connect using credentials stored in streamlit secrets
conn = snowflake.connector.connect(
    user=st.secrets["snowflake"]["user"],
    password=st.secrets["snowflake"]["password"],
    account=st.secrets["snowflake"]["account"],
    warehouse=st.secrets["snowflake"]["warehouse"],
    database=st.secrets["snowflake"]["database"],
    schema=st.secrets["snowflake"]["schema"]
)

cur = conn.cursor()
cur.execute("select current_version()")
version = cur.fetchone()
st.write("connected to snowflake, version:", version)


# get session
# create a snowflake session
session = session.builder.configs({
    "account": st.secrets["snowflake"]["account"],
    "user": st.secrets["snowflake"]["user"],
    "password": st.secrets["snowflake"]["password"],
    "role": st.secrets["snowflake"]["role"],
    "warehouse": st.secrets["snowflake"]["warehouse"],
    "database": st.secrets["snowflake"]["database"],
    "schema": st.secrets["snowflake"]["schema"]
}).create()


# page title
st.title("real time air quality - at station level")
st.write("this streamlit app hosted on snowflake ❄️ made by parth f") 
            
state_option,city_option, station_option, date_option  = '','','',''
state_query = """
    select state from aqi.consumption.location_dim 
    group by state 
    order by 1
"""
state_list = session.sql(state_query).collect()
state_option = st.selectbox('select state',state_list)

#check the selection
if (state_option is not none and len(state_option) > 1):
    city_query = f"""
    select city from aqi.consumption.location_dim 
    where 
    state = '{state_option}' group by city
    order by 1 desc
    """
    city_list = session.sql(city_query).collect()
    city_option = st.selectbox('select city',city_list)

if (city_option is not none and len(city_option) > 1):
    station_query = f"""
    select station from aqi.consumption.location_dim 
        where 
            state = '{state_option}' and
            city = '{city_option}'
        group by station
        order by 1 desc;
    """
    station_list = session.sql(station_query).collect()
    station_option = st.selectbox('select station',station_list)

if (station_option is not none and len(station_option) > 1):
    date_query = f"""
    select date(measurement_time) as measurement_date from aqi.consumption.date_dim
        group by 1 
        order by 1 desc;
    """
    date_list = session.sql(date_query).collect()
    date_option = st.selectbox('select date',date_list)


if (date_option is not none):
    trend_sql = f"""
    select 
        hour(measurement_time) as hour,
        l.state,
        l.city,
        l.station,
        l.latitude::number(10,7) as latitude,
        l.longitude::number(10,7) as longitude,
        pm25_avg,
        pm10_avg,
        so2_avg,
        no2_avg,
        nh3_avg,
        co_avg,
        o3_avg,
        prominent_pollutant,
        aqi
    from 
        aqi.consumption.aqi_fact f 
        join 
        aqi.consumption.date_dim d on d.date_pk  = f.date_fk and date(measurement_time) = '{date_option}'
        join 
        aqi.consumption.location_dim l on l.location_pk  = f.location_fk and 
        l.state = '{state_option}' and
        l.city = '{city_option}' and 
        l.station = '{station_option}'
    order by measurement_time
    """
    sf_df = session.sql(trend_sql).collect()

    df = pd.dataframe(sf_df,columns=['hour','state','city','station','lat', 'lon','pm2.5','pm10','so3','co','no2','nh3','o3','prominent_pollutant','aqi'])
    
    df_aqi = df.drop(['state','city','station','lat', 'lon','pm2.5','pm10','so3','co','no2','nh3','o3','prominent_pollutant'], axis=1)
    df_table = df.drop(['state','city','station','lat', 'lon','prominent_pollutant','aqi'], axis=1)
    df_map = df.drop(['hour','state','city','station','pm2.5','pm10','so3','co','no2','nh3','o3','prominent_pollutant'], axis=1)
    df_stat = df.drop(['state','city','station','lat', 'lon'], axis=1)
    
    def get_aqi_color(aqi):
        if aqi <= 50:
            return "#00b150" 
        elif aqi <= 100:
            return "#96cd5d" 
        elif aqi <= 150:
            return "#ffff00"  
        elif aqi <= 200:
            return "#ffbf00" 
        elif aqi <= 250:
            return "#ff0000" 
        elif aqi <= 400:
            return "#771a83" 
        else:
            return "#96cd5d" 

    st.subheader(f"{station_option}, - aqi : {df_stat['aqi'].iloc[-1]}")
    columns_to_convert = ['lat', 'lon']
    df_map[columns_to_convert] = df_map[columns_to_convert].astype(float)
    df_map['color'] = df_map['aqi'].apply(get_aqi_color)
    st.map(df_map, zoom=13, size='aqi',color='color')
    
    st.subheader(f"hourly aqi levels")
    st.line_chart(df_aqi,x="hour", color = '#ffa500', height=250)

    st.subheader(f"stacked chart:  hourly individual pollutant level")
    st.bar_chart(df_table,x="hour")
    df_stat = df_stat.rename(columns={'prominent_pollutant': 'prominent'})
    st.dataframe(df_stat.iloc[::-1], hide_index=true, height=127, column_order=['hour','pm2.5','pm10','so3','co','no2','nh3','o3','prominent','aqi'])

    st.subheader(f"line chart: hourly pollutant levels")
    st.line_chart(df_table,x="hour")  
   
    # sql statement
    sql_stmt = """
    select l.latitude, l.longitude,f.aqi
            from 
            aqi.consumption.aqi_fact f
            join aqi.consumption.location_dim l on l.location_pk = f.location_fk
        where 
        date_fk = (select date_pk from aqi.consumption.date_dim
        order by measurement_time desc 
        limit 1 )
    """

    # create a data frame
    sf_df = session.sql(sql_stmt).collect()
    
    pd_df =pd.dataframe(
            sf_df,
            columns=['lat','lon','aqi'])

    columns_to_convert = ['lat', 'lon']
    pd_df[columns_to_convert] = pd_df[columns_to_convert].astype(float)
    pd_df['color'] = pd_df['aqi'].apply(get_aqi_color)
    
    st.subheader(f"stations all over india")
    st.map(pd_df,zoom=4 ,size='aqi',color='color')
    st.image("indian_aqi_scale.png", caption="indian air quality scale", use_container_width=true)
