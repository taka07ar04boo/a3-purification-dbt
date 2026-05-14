-- test_governance_tyb_coverage.sql
-- ガバナンスチェック B-06 移植: TYB直前特徴量カバレッジ >= 5カラム
-- a3_governance.py --preflight の B-06 に相当
-- 0行返却 = PASS, 1行以上 = FAIL

WITH tyb_count AS (
    SELECT COUNT(*) AS tyb_cols
    FROM information_schema.columns
    WHERE table_schema = 'api'
      AND table_name = 'a3_features_today_base'
      AND (column_name LIKE '%tyb%' OR column_name LIKE '%chokuzen%' OR column_name LIKE '%tt_%')
)
SELECT tyb_cols
FROM tyb_count
WHERE tyb_cols < 5
