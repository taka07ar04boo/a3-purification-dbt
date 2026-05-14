-- test_governance_taketube_columns.sql
-- TakeTube(TT)特徴量がa3_features_today_baseに存在することを確認
-- feat_tt_* カラムが最低5つ以上存在しなければFAIL
-- (コア憲法 Rule 12: データ資産の不可侵 に準拠)

SELECT 1
WHERE (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = 'api'
      AND table_name = 'a3_features_today_base'
      AND column_name LIKE 'feat_tt_%'
) < 5
