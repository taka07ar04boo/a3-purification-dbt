-- test_governance_sire_bms_nonzero.sql
-- G-09: sire/bms system code の非ゼロ率が80%以上であることを保証
-- Phase 413: governance_preflightからdbt testに移植
-- 非レースデー(0行)の場合はSKIP扱い(0行返却=PASS)

WITH stats AS (
    SELECT
        COUNT(*) AS total,
        COUNT(CASE WHEN COALESCE(feat_sire_system_code, 0) != 0 THEN 1 END) AS sire_nonzero,
        COUNT(CASE WHEN COALESCE(feat_bms_system_code, 0) != 0 THEN 1 END) AS bms_nonzero
    FROM {{ ref('a3_features_today_base') }}
)
SELECT
    total,
    sire_nonzero,
    bms_nonzero,
    CASE WHEN total > 0 THEN ROUND(100.0 * sire_nonzero / total, 1) ELSE 100 END AS sire_pct,
    CASE WHEN total > 0 THEN ROUND(100.0 * bms_nonzero / total, 1) ELSE 100 END AS bms_pct
FROM stats
WHERE total > 0
  AND (sire_nonzero * 100.0 / NULLIF(total, 0) < 80
       OR bms_nonzero * 100.0 / NULLIF(total, 0) < 80)
