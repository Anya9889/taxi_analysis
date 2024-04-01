{{ config(
    materialized="table"
) }}


SELECT 
    taxi_id,
    extract(year from date(trip_start_timestamp)) AS year,
    extract(month from date(trip_start_timestamp)) AS month,
    ROUND(SUM(tips), 3) AS tips_sum
FROM {{ source('taxi_trips', 'taxi_trips') }}
GROUP BY 
    taxi_id,
    extract(year from date(trip_start_timestamp)), 
    extract(month from date(trip_start_timestamp))
ORDER BY ROUND(SUM(tips), 3) DESC
