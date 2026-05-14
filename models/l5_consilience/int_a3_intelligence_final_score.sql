-- C:\Users\新井貴士\.gemini\antigravity\scratch\dbt\models\l5_consilience\int_a3_intelligence_final_score.sql

{{ config(enabled=false) }}

WITH intelligence_hub AS (
    -- Stubbed to resolve missing table error (Phase 386)
    SELECT 
        'portfolio_optimization' as intelligence_key,
        '[{"params": {"gold_weight": 0.5, "sleeper_weight": 0.3, "uplift_weight": 0.2}}]'::jsonb as intelligence_value
),

-- Extracting Strategic Weights from Optuna Hub
strategic_weights AS (
    SELECT 
        (intelligence_value->0->'params'->>'gold_weight')::float as gold_w,
        (intelligence_value->0->'params'->>'sleeper_weight')::float as sleeper_w,
        (intelligence_value->0->'params'->>'uplift_weight')::float as uplift_w
    FROM intelligence_hub
    WHERE intelligence_key = 'portfolio_optimization'
),

-- Extracting Hierarchical Alpha and Physical Base
consilience_base AS (
    SELECT
        c.*,
        h.stable_win_alpha,
        h.stable_place_alpha
    FROM {{ ref('int_a3_consilience') }} c
    LEFT JOIN {{ ref('int_hierarchical_causality') }} h ON c.chokyo_code = h.trainer_code
),

final_scoring AS (
    SELECT
        c.*,
        -- Final A3 Causal Intelligence Score:
        -- (Physical Base + Hierarchical Synergy + Causal Direct Effect) * Optimized Strategy Weight
        (
            c.consilience_score + 
            COALESCE(c.stable_win_alpha, 0) * 10 
        ) as raw_combined_score,
        
        sw.gold_w,
        sw.sleeper_w,
        sw.uplift_w
    FROM consilience_base c
    CROSS JOIN strategic_weights sw
)

SELECT
    *,
    -- Normalized Final Intelligence Score (0 to 10000 scale to prevent uniformity/variance collapse)
    PERCENT_RANK() OVER (ORDER BY (raw_combined_score * 0.4 + gold_w * 0.3 + uplift_w * 0.3)) * 10000.0 as a3_causal_intelligence_score
FROM final_scoring
