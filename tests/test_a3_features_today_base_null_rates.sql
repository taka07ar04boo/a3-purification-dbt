-- api.a3_features_today_base の tt_bias_score と jvd_cushion_val のNULL率が50%を超えている場合にフェイルするテスト
-- レース非開催日の0行時はパスするよう考慮
WITH base_stats AS (
    SELECT 
        COUNT(*) as total_rows,
        SUM(CASE WHEN feat_tt_bias_score IS NULL THEN 1 ELSE 0 END) as null_tt_bias,
        SUM(CASE WHEN feat_jra_cushion_val IS NULL THEN 1 ELSE 0 END) as null_jvd_cushion
    FROM {{ source('api', 'a3_features_today_base') }}
)
SELECT *
FROM base_stats
WHERE total_rows > 0
  AND (
    (null_tt_bias * 1.0 / total_rows) > 0.5
    OR (null_jvd_cushion * 1.0 / total_rows) > 0.5
  )
