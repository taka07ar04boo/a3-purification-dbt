-- test_governance_br_health.sql
-- ガバナンスチェック BR FAILED件数 < 50 (24h以内) を dbt test に移植
-- 0行返却 = PASS, 1行以上 = FAIL

WITH br_failed AS (
    SELECT COUNT(*) AS failed_count
    FROM a3_meta.a3_sub_tasks
    WHERE status = 'FAILED'
      AND created_at > NOW() - INTERVAL '24 hours'
)
SELECT failed_count
FROM br_failed
WHERE failed_count >= 50
