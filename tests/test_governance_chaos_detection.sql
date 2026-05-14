-- test_governance_chaos_detection.sql
-- Phase 405: Chaos検知の無効化再発防止テスト
-- v316でis_chaos=Trueが最低1件は存在することを保証
-- (Chaos検知が完全に無効化されている場合、is_chaos=True: 0件になる)
-- Phase 405で発見: topological_chaos < -10 と mad_pace_resistance > 20 の閾値が
-- 値域的に到達不可能(STDDEV >= 0, MAX=2.62)でChaos検知が100%無効化されていた

SELECT 'chaos_detection' AS check_name, COUNT(*) AS chaos_count
FROM {{ ref('v_inference_today_v316') }}
WHERE is_chaos = true
HAVING COUNT(*) < 1
