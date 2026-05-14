-- dbt/models/l1_micro/l1c_point/int_horse_point_condition.sql
-- L1c: 個体・瞬間 (Micro - Point/Status)
-- 現在の「デキ」、馬体重、調教状態などの「今」の状態を数値化する層。
-- ※未来知識 (Victgrab/NE) は L4 に集約し、ここでは使用しない。

{{ config(enabled=true) }}

WITH training AS (
    SELECT 
        horse_id,
        MAX(training_at) as last_training_at,
        AVG(time_1f) as avg_final_lap
    FROM {{ ref('stg_jra_training') }}
    GROUP BY 1
),
horse_base AS (
    SELECT 
        horse_id,
        horse_name
    FROM {{ ref('stg_jra_horse') }}
),
point_evaluation AS (
    SELECT
        h.horse_id,
        h.horse_name,
        t.last_training_at,
        -- 調教タイムに基づく暫定的なデキスコア (例: ラップが速いほど高評価)
        COALESCE(15.0 - (t.avg_final_lap / 1.0), 0.0) as immediate_form_score
    FROM horse_base h
    LEFT JOIN training t ON h.horse_id = t.horse_id
)
SELECT * FROM point_evaluation
