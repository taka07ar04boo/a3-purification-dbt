-- dbt/models/l7_evolution/int_jockey_trainer_synergy.sql
-- L41: 騎手×厩舎×物理指数 相乗効果分析 (Jockey-Trainer Synergy)
-- 特定のコンビと物理的能力（A3_Physical_Index）の相関を抽出し、期待値の歪みを特定。

{{ config(materialized='table') }}
{{ config(enabled=true) }}

WITH base_results AS (
    SELECT 
        *,
        chokyoshi_code as trainer_code
    FROM {{ ref('int_a3_physical_index') }}
),
synergy_stats AS (
    SELECT
        kishu_code,
        trainer_code,
        -- 指数が高い時（ポテンシャルがある時）の信頼度
        AVG(CASE WHEN a3_raw_physical_index >= 85 THEN 1 ELSE 0 END) as high_ptr_win_rate,
        -- 指数が低いのに勝負してくる「意図」の強さ
        AVG(CASE WHEN a3_raw_physical_index < 70 AND a3_raw_physical_index > 0 THEN 1 ELSE 0 END) as low_ptr_upset_rate,
        -- 市場を裏切る強さ（平均RRS）
        AVG(CASE WHEN a3_raw_physical_index > 0 THEN (a3_raw_physical_index - 80) ELSE 0 END) as avg_index_delta,
        COUNT(*) as combo_count
    FROM base_results
    GROUP BY 1, 2
)
SELECT 
    *,
    -- シナジー・スコアの定義
    -- コンビ回数が一定以上で、高いポテンシャルを結果に繋げているコンビを優遇
    (high_ptr_win_rate * 50 + avg_index_delta * 0.5) as synergy_reliability_score
FROM synergy_stats
WHERE combo_count >= 3
