-- test_governance_jvd_data_coverage.sql
-- Phase 404: JVDテーブルのデータ存在監視
-- jvd_ra/hr/o1~o6にデータが入っていることを確認
-- (jvd_ks/ch/um/hcは別dataspecで取得待ちのため除外)

WITH jvd_counts AS (
    SELECT 'jvd_ra' AS table_name, COUNT(*) AS row_count FROM {{ source('public', 'jvd_ra') }}
    UNION ALL
    SELECT 'jvd_hr', COUNT(*) FROM {{ source('public', 'jvd_hr') }}
    UNION ALL
    SELECT 'jvd_o1', COUNT(*) FROM {{ source('public', 'jvd_o1') }}
    UNION ALL
    SELECT 'jvd_se', COUNT(*) FROM {{ source('public', 'jvd_se') }}
)
SELECT table_name, row_count
FROM jvd_counts
WHERE row_count = 0
