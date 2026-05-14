-- dbt/models/l1_micro/l1b_line/int_wakuren_targets.sql

WITH horse_wakuban AS (
    SELECT
        race_date,
        race_key,
        umaban,
        actual_rank,
        target_umaren,
        {{ calc_wakuban('total_horses', 'umaban') }} AS wakuban
    FROM {{ ref('stg_sed_parsed') }}
    WHERE total_horses IS NOT NULL AND umaban IS NOT NULL
),
winning_wakubans AS (
    SELECT DISTINCT race_key, wakuban
    FROM horse_wakuban
    WHERE target_umaren = 1 AND wakuban IS NOT NULL
)
SELECT
    h.race_date,
    h.race_key,
    h.umaban,
    h.wakuban,
    CASE WHEN w.wakuban IS NOT NULL THEN 1 ELSE 0 END AS target_wakuren
FROM horse_wakuban h
LEFT JOIN winning_wakubans w 
    ON h.race_key = w.race_key AND h.wakuban = w.wakuban
WHERE h.wakuban IS NOT NULL
