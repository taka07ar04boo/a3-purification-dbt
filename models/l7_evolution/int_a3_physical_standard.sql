-- dbt/models/l7_evolution/int_a3_physical_standard.sql
-- L39: A3 基準タイム生成 (Rolling Standard Baseline)
-- 過去全データから、各コース・距離における「1着馬の平均的な走破タイム」を算出。
-- [MMSSf] 形式を [Seconds] にデコードして使用。

{{ config(materialized='table') }}

{{ config(enabled=true) }}

WITH winners AS (
    SELECT
        se.keibajo_code,
        CASE WHEN ra.kyori ~ '^[0-9]+$' THEN CAST(ra.kyori AS INT) ELSE NULL END as distance,
        CASE 
            WHEN ra.track_code ~ '^[0-9]+$' AND CAST(ra.track_code AS INT) <= 20 THEN ra.baba_jotai_code_shiba 
            WHEN ra.track_code ~ '^[0-9]+$' THEN ra.baba_jotai_code_dirt
            ELSE NULL
        END as baba_jotai_code,
        se.kaisai_nen,
        -- Decode '1012' -> 61.2 / '3092' -> 189.2
        CASE WHEN se.soha_time ~ '^[0-9]+$' 
             THEN (CAST(se.soha_time AS INT) / 1000) * 60 + (MOD(CAST(se.soha_time AS INT), 1000) / 10.0)
             ELSE NULL 
        END as time_sec
    FROM {{ source('public', 'jvd_se') }} se
    INNER JOIN {{ source('public', 'jvd_ra') }} ra
        ON se.kaisai_nen = ra.kaisai_nen 
       AND se.kaisai_tsukihi = ra.kaisai_tsukihi 
       AND se.keibajo_code = ra.keibajo_code 
       AND se.race_bango = ra.race_bango
    WHERE se.kakutei_chakujun = '01' 
      AND se.soha_time != '0000'
      AND se.kaisai_nen >= '2005'
),
standards AS (
    SELECT
        keibajo_code,
        distance,
        baba_jotai_code,
        AVG(time_sec) as standard_time_sec,
        COUNT(*) as sample_count
    FROM winners
    WHERE distance IS NOT NULL AND time_sec IS NOT NULL AND baba_jotai_code IS NOT NULL
    GROUP BY 1, 2, 3
)
SELECT * FROM standards
WHERE sample_count >= 5 -- サンプル不足の極端なケースを除外
