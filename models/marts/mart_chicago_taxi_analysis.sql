{{ config(
    materialized="incremental", 
    unique_key=['taxi_id','year', 'month']
) }}


SELECT 
    td.taxi_id,
    td.year,
    td.month,
    td.tips_sum,
ROUND(
    (td.tips_sum - LAG(td.tips_sum, 1, 0) OVER (partition by taxi_id ORDER BY td.year, td.month)) /
    NULLIF(LAG(td.tips_sum, 1, 0) OVER (partition by taxi_id ORDER BY td.year, td.month), 0),
    2
) AS tips_change

FROM 
    {{ ref('stg_taxi_tips_sum') }} as td
where td.taxi_id in (select taxi_id from {{ ref('stg_top_drivers') }})
and ((year = 2018 and month >= 4) or year = 2019)


{% if is_incremental() %}
    AND (td.year = (SELECT MAX(year) FROM {{ this }}) AND td.month = (SELECT MAX(month) FROM {{ this }}))
{% endif %}


ORDER BY 
    taxi_id,
    td.year,
    td.month
