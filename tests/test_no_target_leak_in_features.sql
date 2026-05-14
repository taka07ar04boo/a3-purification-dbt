-- R-WL: Feature Whitelist Test (Phase 360: a3_features_today_baseに更新)
-- ターゲット変数が推論テーブルに混入していないことを確認 (DGRD-17)
-- 注意: 学習MATVIEW(v56)にはターゲット列が必要なのでチェック対象外
-- 推論テーブルは推論時に使うため、ターゲット列が含まれてはならない
SELECT 
  attname as leaked_column
FROM pg_attribute 
WHERE attrelid = 'api.a3_features_today_base'::regclass 
  AND attnum > 0 
  AND NOT attisdropped
  AND attname IN ('actual_rank', 'is_winner', 'is_top3', 'is_top5', 'kakutei_chakujun', 'payout_win', 'payout_umaren')
