-- dbt/models/l6_strategy/a3_portfolio_gold_v2.sql

{{ config(enabled=true) }}

WITH consilience AS (
    SELECT * FROM {{ ref('int_a3_consilience') }}
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY target_at ORDER BY consilience_score DESC) as rank_in_day
    FROM consilience
    WHERE consilience_score > 0
),
final_selection AS (
    SELECT
        target_at,
        venue,
        race,
        horse_no,
        horse_name,
        consilience_score,
        rank_in_day
    FROM ranked
    -- 1日のうち、スコア上位8頭に絞り込む（ROI最適化）
    WHERE rank_in_day <= 8
)
SELECT * FROM final_selection
