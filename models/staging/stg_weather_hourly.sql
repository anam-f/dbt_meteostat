WITH hourly_raw AS (
    SELECT airport_code,
           station_id,
           JSON_ARRAY_ELEMENTS(extracted_data -> 'data') AS json_data
    FROM  {{ source('weather_data', 'weather_hourly_raw') }}
),
hourly_flattened AS (
    SELECT airport_code,
           station_id,
           (json_data ->> 'coco')::NUMERIC AS weather_condition_code,
           (json_data ->> 'dwpt')::NUMERIC AS dew_point,
           (json_data ->> 'prcp')::NUMERIC AS precipitation_mm,
           (json_data ->> 'pres')::NUMERIC AS sea_level_air_pressure,
           (json_data ->> 'rhum')::NUMERIC AS relative_humidity,
           (json_data ->> 'snow')::NUMERIC::INTEGER AS max_snow_mm,
           (json_data ->> 'temp')::NUMERIC AS air_temperature,
           (json_data ->> 'time')::timestamp AS date_time,
           (json_data ->> 'tsun')::NUMERIC AS sunshine_duration,
           (json_data ->> 'wdir')::NUMERIC::INTEGER AS wind_direction,
           (json_data ->> 'wspd')::NUMERIC AS wind_speed,
           (json_data ->> 'wpgt')::NUMERIC AS peakgust
    FROM  hourly_raw
)
SELECT * FROM hourly_flattened