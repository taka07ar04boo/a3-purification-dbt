-- test_governance_recurring_count.sql  
-- ガバナンスチェック: BR RECURRING パターン数 >= 50
-- 0行返却 = PASS, 1行以上 = FAIL

WITH recurring_count AS (
    SELECT COUNT(DISTINCT sub_task_name) AS pattern_count
    FROM a3_meta.a3_sub_tasks
    WHERE is_recurring = true
)
SELECT pattern_count
FROM recurring_count
WHERE pattern_count < 50
