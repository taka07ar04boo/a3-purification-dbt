-- test_governance_chaos_ratio.sql
-- Phase 406: Chaos比率の妥当性チェック
-- is_chaos=True が全体の0.01%〜20%の範囲であることを保証
-- 0%: Chaos検知が無効化(Phase 405で発見されたバグの再発)
-- 20%超: 閾値が緩すぎてChaosルーティングが意味をなさない
-- 0行返却 = PASS, 1行以上 = FAIL

WITH chaos_stats AS (
    SELECT 
        COUNT(*) AS total,
        SUM(CASE WHEN is_chaos THEN 1 ELSE 0 END) AS chaos_count,
        ROUND(100.0 * SUM(CASE WHEN is_chaos THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS chaos_pct
    FROM {{ ref('v_inference_today_v316') }}
)
SELECT chaos_pct, total, chaos_count
FROM chaos_stats
WHERE total > 100  -- Only check when sufficient data exists
  AND (chaos_pct = 0 OR chaos_pct > 20.0)
