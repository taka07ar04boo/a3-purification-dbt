-- test_governance_regime_distribution.sql
-- Phase 406: Regime分布の健全性チェック
-- v316のregimeカラムに最低4種類のレジームが存在することを保証
-- (Core_Turf, Core_Dirt, Sprint_Turf/Sprint_Dirt, Chaos_Turf/Chaos_Dirt, Hurdle)
-- 3種類以下の場合はロジックエラーの疑い
-- 0行返却 = PASS, 1行以上 = FAIL

WITH regime_counts AS (
    SELECT COUNT(DISTINCT regime) AS regime_variety
    FROM {{ ref('v_inference_today_v316') }}
    WHERE regime IS NOT NULL
)
SELECT regime_variety
FROM regime_counts
WHERE regime_variety < 4
  AND regime_variety IS NOT NULL  -- NULL = empty view (non-race day), not an error
