-- test_governance_br_orphan_dependency.sql
-- BR依存チェーン整合性: FAILEDのまま後続タスクが待機していないことを保証
-- Phase 413: governance_preflightからdbt testに移植

SELECT
    st.sub_task_id,
    st.sub_task_name,
    st.status,
    st.depends_on_task_id
FROM a3_meta.a3_sub_tasks st
WHERE st.depends_on_task_id IS NOT NULL
  AND st.depends_on_task_id IN (
      SELECT sub_task_id FROM a3_meta.a3_sub_tasks WHERE status = 'FAILED'
  )
  AND st.status NOT IN ('COMPLETED', 'ARCHIVED', 'CANCELLED', 'FAILED')
