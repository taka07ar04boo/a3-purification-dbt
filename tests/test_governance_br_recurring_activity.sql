-- test_governance_br_recurring_activity: BR RECURRINGタスクが24h以内に50件以上完了していることを保証
-- BR自動化エンジンの活性（liveness）チェック
-- Phase 419: BR automation liveness verification
SELECT 1
WHERE (
    SELECT COUNT(*)
    FROM a3_meta.a3_sub_tasks
    WHERE status = 'COMPLETED'
      AND updated_at > NOW() - INTERVAL '24 hours'
      AND sub_task_name LIKE '%recurring%'
) < 50
