{{ config(
    materialized="table"
) }}


SELECT 
taxi_id
from {{ ref ('stg_taxi_tips_sum') }}
WHERE 
year = 2018 and month = 4
limit 3
    