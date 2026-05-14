-- test_governance_feature_count.sql
-- ガバナンスチェック B-09 移植: a3_features_today_base の特徴量カラム数が70以上であること
-- a3_governance.py --preflight の B-09 に相当
-- 0行返却 = PASS, 1行以上 = FAIL

WITH col_count AS (
    SELECT COUNT(*) AS feat_cols
    FROM information_schema.columns
    WHERE table_schema = 'api'
      AND table_name = 'a3_features_today_base'
      AND column_name LIKE 'feat_%'
)
SELECT feat_cols
FROM col_count
WHERE feat_cols < 70
