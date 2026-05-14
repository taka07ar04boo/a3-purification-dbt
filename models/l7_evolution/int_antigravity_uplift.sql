-- dbt/models/l7_evolution/int_antigravity_uplift.sql
-- L42: アップリフト・レゾナンス (Human Alpha Isolation)
-- 物理的ポテンシャル（A3_Index）に対する、特定のコンビ（T=1）による「能力の純増分」を推計。

{{ config(materialized='table') }}

{{ config(enabled=true) }}

WITH base_data AS (
    SELECT 
        i.*,
        i.chokyoshi_code as trainer_code,
        sy.synergy_reliability_score,
        -- 介入の定義: シナジーが高い（40以上）を T=1, それ以外を T=0
        CASE WHEN COALESCE(sy.synergy_reliability_score, 0) > 40 THEN 1 ELSE 0 END as treatment
    FROM {{ ref('int_a3_physical_index') }} i
    LEFT JOIN {{ ref('int_jockey_trainer_synergy') }} sy 
        ON i.chokyoshi_code = sy.trainer_code 
    -- [Optimization] We only need the uplift score for current/recent focus
    WHERE i.target_at >= '2024-01-01'
),
-- Pre-computed Long-term Baseline from Materialized Table
baseline_stats AS (
    SELECT
        keibajo_code,
        distance,
        baba_jotai_code,
        avg_long_term_index as avg_control_index,
        std_long_term_index as std_control_index
    FROM {{ ref('int_a3_long_term_baseline') }}
),
-- 介入群 (Treatment: T=1) の走破偏差
treatment_performance AS (
    SELECT
        b.target_at,
        b.keibajo_code,
        b.race_bango,
        b.umaban,
        b.treatment,
        b.trainer_code,
        b.a3_raw_physical_index,
        -- 対照群(物理標準)からの浮き上がりを Uplift と定義
        (b.a3_raw_physical_index - bs.avg_control_index) as raw_uplift
    FROM base_data b
    LEFT JOIN baseline_stats bs 
        ON b.keibajo_code = bs.keibajo_code 
       AND b.distance = bs.distance 
       AND b.baba_jotai_code = bs.baba_jotai_code
    WHERE b.treatment = 1
),
-- Masses' Memory: Recent average performance for comparison
mass_perception AS (
    SELECT
        b.trainer_code,
        AVG(b.a3_raw_physical_index) as recent_perception_index
    FROM base_data b
    -- Masses focus on the latest results
    WHERE b.target_at >= (CURRENT_DATE - INTERVAL '12 months')::text
    GROUP BY 1
)
SELECT 
    tp.*,
    mp.recent_perception_index,
    -- The Forgetting Gap: Deviation between 20-year True Capability and Recent Perception
    (tp.a3_raw_physical_index - mp.recent_perception_index) as forgetting_gap_alpha,
    (tp.raw_uplift * 2.0) as uplift_resonance_score
FROM treatment_performance tp
LEFT JOIN mass_perception mp ON tp.trainer_code = mp.trainer_code
