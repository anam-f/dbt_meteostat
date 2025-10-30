with daily_data as (
    SELECT *
    FROM {{ ref('stg_weather_daily') }}
),
add_features as ( 
    SELECT *
            ,date_part('day', date)::INTEGER as date_day
            ,date_part('month',date)::INTEGER as date_month
            ,date_part('year', date)::INTEGER as date_year
            ,date_part('week', date)::INTEGER as calendar_week
            ,to_char(date, 'FMmonth') as month_name
            ,to_char(date, 'FMday') as day_of_the_week
    FROM daily_data
),
add_more_features as (
    SELECT *
    ,CASE 
            when month_name in ('december', 'january', 'february') then 'winter'
            when month_name in ('march', 'april', 'may') then 'spring'
            when month_name in ('june', 'july', 'august') then 'summer'
            when month_name in ('september', 'october','november') then 'autumn'
    END as season
    from add_features 
)
SELECT *
from add_more_features
ORDER by date