-- dbt/models/l7_evolution/int_a3_physical_index.sql
-- L40: A3 物理的絶対指数 (Custom Physical Index)
-- 個々の馬の [soha_time] を L39 の基準と比較し、斤量を加味した指数に変換。
{{ config(materialized='table', enabled=true) }}

WITH base_results AS (
    SELECT
        se.kaisai_nen, se.kaisai_tsukihi, se.keibajo_code, se.race_bango, se.umaban,
        se.chokyoshi_code, se.kishu_code, se.barei as age, ra.track_code,
        (se.kaisai_nen || '-' || SUBSTR(se.kaisai_tsukihi, 1, 2) || '-' || SUBSTR(se.kaisai_tsukihi, 3, 2)) as target_at,
        CASE WHEN ra.kyori ~ '^[0-9]+$' THEN CAST(ra.kyori AS INT) ELSE NULL END as distance,
        CASE 
            WHEN ra.track_code ~ '^[0-9]+$' AND CAST(ra.track_code AS INT) <= 20 THEN ra.baba_jotai_code_shiba 
            WHEN ra.track_code ~ '^[0-9]+$' THEN ra.baba_jotai_code_dirt
            ELSE NULL
        END as baba_jotai_code,
        CASE WHEN se.futan_juryo ~ '^[0-9.]+$' THEN CAST(se.futan_juryo AS FLOAT) ELSE NULL END as weight,
        -- MMSSf -> Seconds
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
    WHERE se.soha_time != '0000'
),
standards AS (
    SELECT * FROM {{ ref('int_a3_physical_standard') }}
),
scored AS (
    SELECT
        b.*,
        s.standard_time_sec,
        -- [Speed Figure Formula]
        -- (Standard - HorseTime) * 10 + 80 + (Weight - 55) * 2
        -- ※タイムが速い（基準より小さい）ほど指数が高くなるよう引き算。
        (s.standard_time_sec - b.time_sec) * 10 
            + 80 
            + (b.weight - 55.0) * 2.0 as a3_raw_physical_index
    FROM base_results b
    INNER JOIN standards s 
        ON b.keibajo_code = s.keibajo_code 
       AND b.distance = s.distance 
       AND b.baba_jotai_code = s.baba_jotai_code
    WHERE b.distance IS NOT NULL AND b.time_sec IS NOT NULL AND b.weight IS NOT NULL
)
SELECT * FROM scored
