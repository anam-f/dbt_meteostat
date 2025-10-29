
SELECT *
FROM  {{ source('flights_data','airports') }}
JOIN {{source ('flights_data','regions1')}} on airports.country = regions.country 