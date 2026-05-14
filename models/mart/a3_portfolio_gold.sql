-- models/mart/a3_portfolio_gold.sql
-- A3 ポートフォリオ生成ロジック (Consilience v3.5 + SSI Learning)
-- 分解・高速化バージョン。スカラサブクエリを CROSS JOIN に置換。

{{ config(enabled=true) }}

WITH ne_data AS (
    SELECT * FROM {{ ref('stg_ne_rankings') }}
),
ssi AS (
    -- L7: 自己進化層から算出された最新の SSI を取得
    SELECT 
        CAST(win_rate AS NUMERIC) as ssi_score
    FROM {{ ref('int_ssi_dynamic_weights') }}
    WHERE source_layer = 'L4_Victgrab' AND confidence_level = 'HIGH'
    LIMIT 1
),
scored AS (
    SELECT
        ne.*,
        COALESCE(s.ssi_score, 0.05) as ssi_mult, -- 的中実績に基づく動的マルチプライヤ
        -- スコア計算 (未来知識に基づく期待値)
        ((ne.yusens * 0.3) +
        (CASE 
            WHEN ne.yusens_rank = 1 THEN 36.0
            WHEN ne.yusens_rank = 2 THEN 24.0
            WHEN ne.yusens_rank = 3 THEN 12.0
            ELSE 0.0
         END) +
        (ne.has_fu2 * 18.0) +
        (CASE WHEN ne.f_index > 80 THEN (ne.f_index - 80) * 0.6 ELSE 0.0 END) +
        (ne.has_r1 * 6.0) + (ne.has_r2 * 3.0)) 
        * (1.0 + COALESCE(s.ssi_score, 0) * 10.0) as base_score
    FROM ne_data ne
    CROSS JOIN ssi s -- スカラサブクエリから CROSS JOIN へ変更して高速化
),
final_calculation AS (
    SELECT
        *,
        -- 計算結果の丸め
        ROUND(base_score::NUMERIC, 2) as final_score
    FROM scored
    WHERE base_score > 0
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY target_at ORDER BY final_score DESC) as rank_in_day
    FROM final_calculation
)
SELECT * FROM ranked WHERE rank_in_day <= 8
