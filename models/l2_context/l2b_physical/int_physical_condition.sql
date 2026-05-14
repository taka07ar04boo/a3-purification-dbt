-- dbt/models/l2_context/l2b_physical/int_physical_condition.sql

-- int_physical_condition

WITH cushion AS (
    SELECT * FROM {{ ref('stg_jra_cushion') }}
),
races AS (
    SELECT 
        kaisai_nen || '-' || SUBSTRING(kaisai_tsukihi, 1, 2) || '-' || SUBSTRING(kaisai_tsukihi, 3, 2) as target_at,
        keibajo_code as venue,
        baba_jotai_code_shiba as track_condition_code,
        tenko_code
    FROM {{ source('public', 'jvd_ra') }}
    GROUP BY 1, 2, 3, 4
)
SELECT 
    r.*,
    c.cushion_value
FROM races r
LEFT JOIN cushion c ON r.target_at = c.target_at AND r.venue = c.venue
