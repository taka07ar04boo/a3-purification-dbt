-- dbt/models/l7_evolution/int_ssi_dynamic_weights.sql
-- L7: SSI 動的重み付け (SSI Dynamic Weighting)
-- 監査結果に基づき、各層の信頼度（SSI）を動的に算出する。
-- A3 が「今、どの層の知能を重視すべきか」を決定する司令塔。

{{ config(enabled=true) }}

WITH audit AS (
    SELECT * FROM {{ ref('int_self_audit') }}
),
-- 層ごとの的中性能（SSI）を算出
dynamic_ssi AS (
    SELECT
        source_layer,
        (CASE 
            WHEN predicted_score >= 80 THEN 'HIGH'
            WHEN predicted_score >= 50 THEN 'MID'
            ELSE 'LOW'
        END) as confidence_level,
        COUNT(*) as sample_size,
        -- 勝率 (is_win) および 平均着順誤差 (rank_error) から SSI を算出
        SUM(is_win)::float / NULLIF(COUNT(*), 0) as win_rate,
        AVG(rank_error) as avg_error,
        -- SSI: 的中率が高いほど、誤差が少ないほど高くなる重み
        (SUM(is_win)::float / NULLIF(COUNT(*), 0) * 100.0) / (1.0 + AVG(rank_error)) as ssi_weight
    FROM audit
    GROUP BY 1, 2
)
SELECT 
    *,
    RANK() OVER (PARTITION BY confidence_level ORDER BY ssi_weight DESC) as ssi_rank
FROM dynamic_ssi
WHERE sample_size >= 10 -- 的計的な信頼性を担保
