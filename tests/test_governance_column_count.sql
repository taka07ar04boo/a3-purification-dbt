-- test_governance_column_count.sql
-- a3_features_today_base の必須カラム数を確認
-- Phase 402時点で259カラム。下限220以上を保証。
-- 大幅なカラム削減はデグレードの兆候。

SELECT 1
WHERE (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = 'api'
      AND table_name = 'a3_features_today_base'
) < 220
