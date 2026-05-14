-- dbt/models/l2_context/l2d_dynamics/int_race_dynamics.sql

WITH scores AS (
    SELECT 
        target_at,
        venue,
        race,
        horse_no,
        yusens
    FROM {{ ref('stg_ne_rankings') }}
),
stats AS (
    SELECT
        target_at,
        venue,
        race,
        COUNT(*) as runner_count,
        STDDEV(yusens) as ability_dispersion,
        AVG(yusens) as avg_field_ability
    FROM scores
    GROUP BY 1, 2, 3
),
dynamics AS (
    SELECT
        *,
        -- 頭数によるプレッシャー係数（多頭数ほど高い）
        CASE 
            WHEN runner_count >= 16 THEN 1.5
            WHEN runner_count >= 12 THEN 1.2
            ELSE 1.0
        END as density_pressure,
        -- 分散による波乱度（L1の能力が拮抗しているほど高い）
        CASE 
            WHEN ability_dispersion < 10 THEN 'HIGH_VOLATILITY' -- 拮抗
            ELSE 'STABLE'
        END as field_stability
    FROM stats
)
SELECT * FROM dynamics
