-- test_chaos_threshold_db_sync.sql
-- Phase 415: Chaos閾値のDB一元管理テーブル整合性検証
-- a3_meta.chaos_thresholdsに正規閾値が4件登録されていることを確認
-- 0行返却 = PASS

WITH threshold_check AS (
    SELECT COUNT(*) AS cnt
    FROM a3_meta.chaos_thresholds
    WHERE threshold_key IN (
        'race_std_score_idm',
        'race_std_idx_ten',
        'topological_chaos',
        'mad_pace_resistance'
    )
)
SELECT cnt
FROM threshold_check
WHERE cnt < 4
