{{ config(
    materialized='view',
    schema='api'
) }}

SELECT sovereign_rules.rule_id,
    sovereign_rules.rule_name,
    sovereign_rules.description AS rule_text,
    sovereign_rules.severity,
    sovereign_rules.check_phase AS enforcement,
    sovereign_rules.created_at
FROM a3_meta.sovereign_rules
WHERE sovereign_rules.is_active = true
