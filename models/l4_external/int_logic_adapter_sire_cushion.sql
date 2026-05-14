-- dbt/models/l4_external/int_logic_adapter_sire_cushion.sql

{{ config(enabled=true) }}

WITH base AS (
    SELECT 
        h.horse_id,
        h.sire_id,
        p.cushion_value,
        p.track_condition_code
    FROM {{ ref('stg_jra_horse') }} h
    CROSS JOIN {{ ref('int_physical_condition') }} p
),
-- 実装例: 特定の血統（系統）が硬い馬場に強いという仮説をスコアリング
logic_calculation AS (
    SELECT
        horse_id,
        CASE 
            WHEN cushion_value >= 9.5 AND sire_id IN ('0001', '0002') THEN 1.0 -- 仮のID
            ELSE 0.5
        END as logic_score
    FROM base
)
SELECT 
    'DUMMY_HORSE' as horse_id,
    'SIRE_CUSHION_ADAPT' as logic_id,
    1.0 as logic_score,
    CURRENT_TIMESTAMP as calculated_at
LIMIT 0
