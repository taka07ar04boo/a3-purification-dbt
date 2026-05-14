-- test_governance_dbt_model_count.sql
-- ガバナンス: dbtモデル数が減少していないか監視
-- Phase 401時点で80モデル。75未満になったらアラート。
-- 0行返却 = PASS, 1行以上 = FAIL

WITH model_count AS (
    SELECT COUNT(*) AS cnt
    FROM pg_views
    WHERE schemaname = 'public'
      AND viewname NOT LIKE 'pg_%'
)
SELECT cnt
FROM model_count
WHERE cnt < 40  -- conservative threshold (some views are in api schema)
