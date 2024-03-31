SELECT 
    td.taxi_id,
    td.year,
    td.month,
    td.tips_sum,
    (td.tips_sum - 
    LAG(td.tips_sum, 1, 0) OVER (partition by taxi_id ORDER BY td.year, td.month)) /
    NULLIF(LAG(td.tips_sum, 1, 0) OVER (partition by taxi_id ORDER BY td.year, td.month), 0) AS tips_change
FROM 
    {{ ref ('stg_top_drivers') }} as td
ORDER BY 
    td.tips_sum DESC
