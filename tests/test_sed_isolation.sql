-- R-SED: SED Isolation Test (Phase 360: a3_features_today_baseに更新)
-- 禁止されたSEDカラム(レース後にしか確定しない成績情報)が推論テーブルに存在しないこと
-- DGRD-17 (未来データ混入禁止) の物理的ゲート
SELECT 
  attname as leaked_sed_column
FROM pg_attribute 
WHERE attrelid = 'api.a3_features_today_base'::regclass 
  AND attnum > 0 
  AND NOT attisdropped
  AND attname IN ('kakutei_chakujun', 'halon_time', 'agari_3f', 'race_p_index', 'ten_index', 'pace_index', 'kohan_index', 'time_sa')
