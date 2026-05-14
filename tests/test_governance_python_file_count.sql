-- test_governance_python_file_count.sql
-- ガバナンスチェック DGRD-08: Pythonファイル数 < 300
-- 0行返却 = PASS, 1行以上 = FAIL

WITH file_count AS (
    SELECT COUNT(*) AS py_count
    FROM api.a3_file_registry
    WHERE file_type = 'python'
      AND file_name NOT LIKE 'test_%'
      AND file_name NOT LIKE 'debug_%'
)
SELECT py_count
FROM file_count
WHERE py_count >= 300
