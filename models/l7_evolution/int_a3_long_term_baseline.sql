-- dbt/models/l7_evolution/int_a3_long_term_baseline.sql
-- L41: A3 長期物理ベースライン (Static Baseline Master)
-- 20年分のデータから、各コース環境（競馬場/距離/馬場）における基準値を事前集計し固定化。

{{ config(materialized='table') }}
{{ config(enabled=true) }}

WITH base_data AS (
    SELECT 
        keibajo_code,
        distance,
        baba_jotai_code,
        a3_raw_physical_index,
        -- シナジー計算（後続JOIN用）
        chokyoshi_code
    FROM {{ ref('int_a3_physical_index') }}
)

SELECT
    keibajo_code,
    distance,
    baba_jotai_code,
    AVG(a3_raw_physical_index) as avg_long_term_index,
    STDDEV(a3_raw_physical_index) as std_long_term_index,
    COUNT(*) as sample_count
FROM base_data
GROUP BY 1, 2, 3
HAVING COUNT(*) >= 30 -- 統計的有意性の確保
