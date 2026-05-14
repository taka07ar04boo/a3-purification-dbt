-- api.a3_inference_logs 内の meta_score について、同一日の (最大値 - 最小値) が 1.0 未満（スコア均一化の疑い）である場合にフェイルするテスト
WITH daily_stats AS (
    SELECT 
        DATE(created_at) as log_date,
        COUNT(*) as num_predictions,
        MAX(meta_score) as max_score,
        MIN(meta_score) as min_score
    FROM {{ source('api', 'a3_inference_logs') }}
    GROUP BY DATE(created_at)
)
SELECT *
FROM daily_stats
WHERE num_predictions > 10
  AND (max_score - min_score) < 1.0
