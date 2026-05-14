-- dbt/models/l1_micro/l1b_line/int_horse_training_history.sql

{{ config(enabled=true) }}

WITH training AS (
    SELECT * FROM {{ ref('stg_jra_training') }}
),
stats AS (
    SELECT
        horse_id,
        training_at,
        time_4f,
        time_3f,
        -- 直近3回の調教タイムの平均（WINDOW関数）
        AVG(time_4f) OVER (PARTITION BY horse_id ORDER BY training_at ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as avg_time_4f_last3,
        -- 自己ベスト（4F）
        MIN(time_4f) OVER (PARTITION BY horse_id) as best_time_4f_absolute
    FROM training
)
SELECT * FROM stats
