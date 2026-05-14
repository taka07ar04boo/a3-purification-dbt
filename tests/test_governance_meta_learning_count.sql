-- test_governance_meta_learning_count.sql
-- Phase 404: メタ学習の蓄積量チェック
-- 最低100件以上のメタ学習エントリが存在すること

SELECT 'meta_learning' AS check_name, COUNT(*) AS cnt
FROM {{ source('api', 'system_meta_learning_history') }}
HAVING COUNT(*) < 100
