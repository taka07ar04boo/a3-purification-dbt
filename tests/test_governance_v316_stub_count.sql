-- test_governance_v316_stub_count.sql
-- ガバナンス: v316のスタブ(ハードコード0)が増えていないか監視
-- Phase 401でスタブ12件→1件に削減。Phase 421で最後の1件(feat_heritage_golden_match)も解消。
-- 0行返却 = PASS, 1行以上 = FAIL

WITH view_def AS (
    SELECT pg_get_viewdef('api.v_inference_today_v316'::regclass, true) AS def
),
stub_count AS (
    SELECT 
        (LENGTH(def) - LENGTH(REPLACE(def, '0 AS feat_', ''))) / LENGTH('0 AS feat_') AS approx_stubs
    FROM view_def
)
SELECT approx_stubs
FROM stub_count
WHERE approx_stubs > 0  -- Phase 421: All 12 stubs resolved, zero tolerance
