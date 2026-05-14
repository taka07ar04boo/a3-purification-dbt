-- test_governance_score_variance.sql
-- ガバナンス: 推論スコア(meta_score)の均一化バグ再発防止
-- is_chaos/regime計算にも影響する重要なチェック
-- 直近のスコアのmax-minが10以上であれば正常
-- 0行返却 = PASS, 1行以上 = FAIL

WITH score_range AS (
    SELECT 
        MAX(meta_score) - MIN(meta_score) AS score_spread
    FROM api.a3_inference_logs
    WHERE created_at > NOW() - INTERVAL '7 days'
      AND meta_score IS NOT NULL
)
SELECT score_spread
FROM score_range
WHERE score_spread < 10.0
  AND score_spread IS NOT NULL  -- NULL means no logs in 7 days, not an error
