-- test_governance_jvd_ra_data_quality.sql
-- Phase 404: jvd_raデータのフィールド品質チェック
-- kaisai_nen が 4桁の年として妥当であること (オフセットずれ再発防止)

SELECT keibajo_code, kaisai_nen, kaisai_tsukihi
FROM {{ source('public', 'jvd_ra') }}
WHERE kaisai_nen !~ '^20[0-9]{2}$'
   OR kaisai_tsukihi !~ '^[0-1][0-9][0-3][0-9]$'
   OR keibajo_code !~ '^[0-9]{2}$'
LIMIT 10
