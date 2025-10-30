with flights_data as (
    SELECT * 
    from {{ ref('stg_flights_one_month') }}
),
flights_cleaned as (
    SELECT 
        flight_date::DATE
        ,to_char(dep_time, 'fm0000')::TIME as dep_time
        ,to_char(sched_dep_time, 'fm0000')::time as sched_dep_time
        ,dep_delay
        ,(dep_delay * '1 minute'::interval) AS dep_delay_interval
        ,to_char(arr_time, 'fm0000'):: time as arr_time
        ,to_char(sched_arr_time, 'fm0000')::time as sched_arr_time
        ,arr_delay
        ,(arr_delay * '1 minute'::interval) AS arr_delay_interval
        ,airline
        ,tail_number
        ,flight_number
        ,origin
        ,dest 
        ,air_time
        ,(air_time * '1 minute'::interval) AS air_time_interval
        ,actual_elapsed_time
        ,(actual_elapsed_time * '1 minute'::interval) AS actual_elapsed_time_interval
        ,(distance / 0.621371)::NUMERIC(6,2) AS distance_km
        ,cancelled
        ,diverted
    FROM flights_data
)       
SELECT * from flights_cleaned