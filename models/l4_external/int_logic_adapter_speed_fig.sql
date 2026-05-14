-- dbt/models/l4_external/int_logic_adapter_speed_fig.sql

WITH history AS (
    SELECT 
        ketto_toroku_bango as horse_id,
        CAST(NULLIF(TRIM(kakutei_chakujun), '') AS integer) as rank,
        16 as runner_count
    FROM {{ source('public', 'jvd_se') }}
    WHERE TRIM(kakutei_chakujun) ~ '^[0-9]+$'
),
-- 論理の再現: 「多頭数での好走を高く評価する」という方法論
logic_calculation AS (
    SELECT
        horse_id,
        AVG(rank * (1.0 / runner_count)) as logic_score
    FROM history
    GROUP BY 1
)
SELECT 
    horse_id,
    'SPEED_FIG_SIMPLE' as logic_id,
    logic_score,
    CURRENT_TIMESTAMP as calculated_at
FROM logic_calculation
