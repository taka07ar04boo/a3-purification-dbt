-- test_governance_inference_freshness: 推論ログが直近7日以内に存在することを保証
-- 非レースデー期間が長期化した場合のみ正当にFAILする
-- Phase 419: Inference pipeline liveness check
SELECT 1
WHERE (
    SELECT COUNT(*)
    FROM api.a3_inference_logs
    WHERE created_at > NOW() - INTERVAL '7 days'
) < 1
