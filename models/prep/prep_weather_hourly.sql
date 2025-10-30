{{ config(materialized='table') }}
with hourly_data as (
    SELECT *
    FROM {{ ref('stg_weather_hourly') }}
),
date_time_parts as (
    SELECT *
        ,date_time::DATE as date 
        ,date_time::TIME as time
        ,to_char(date_time, 'HH24:MI') as hour 
        ,to_char(date_time, 'FMmonth') as month_name 
        ,date_part('day', date_time) as date_day 
        ,date_part('month', date_time) as date_month 
        ,date_part('year', date_time) as date_year 
        ,date_part('week', date_time) as calendar_week
    from hourly_data
),
day_part_intervals as (
    SELECT 
        *
        ,(CASE 
            when time BETWEEN '00:00:00' and '05:59:59' then 'night'
            when time BETWEEN '06:00:00' and '17:59:59' then 'day'
            when time BETWEEN '18:00:00' and '23:59:59' then 'evening' 
        end) as day_part
    FROM date_time_parts
)
SELECT * 
from day_part_intervals