-- dbt/models/l4_external/int_external_reliability.sql
-- L4: 外部知能の信頼度 (External Reliability)
-- L7 の監査結果から、外部ソースごとの信頼強度を算出する。

{{ config(enabled=true) }}

WITH audit AS (
    SELECT * FROM {{ ref('int_self_audit') }}
    WHERE source_layer LIKE 'L4_%' -- 外部知能のみを対象
),
reliability_metrics AS (
    SELECT
        source_layer as source_name,
        COUNT(*) as total_samples,
        AVG(rank_error) as avg_error,
        SUM(is_win)::float / NULLIF(COUNT(*), 0) as win_rate
    FROM audit
    GROUP BY 1
),
weighted_opinions AS (
    SELECT
        *,
        -- 誤差が少なく、的中率が高いソースほど重くする
        CASE
            WHEN win_rate > 0.2 AND avg_error < 3 THEN 1.5
            WHEN win_rate > 0.1 THEN 1.0
            ELSE 0.5
        END as source_weight
    FROM reliability_metrics
)
SELECT * FROM weighted_opinions
