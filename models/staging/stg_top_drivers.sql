{{ config(
    materialized="table"
) }}


SELECT 
taxi_id
from {{ ref ('stg_taxi_tips_sum') }}
limit 3
    