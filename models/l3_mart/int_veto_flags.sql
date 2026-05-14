{{ config(materialized='view') }}

WITH base AS (
    SELECT 
        race_key,
        umaban,
        CASE WHEN TRIM(odds_win) ~ '^-?[0-9.]+$' THEN odds_win::numeric ELSE 0 END AS feat_odds_win_pre
    FROM public.kyi_parsed
    WHERE race_date = '{{ var("target_date", "") }}'
)
SELECT 
    race_key,
    umaban,
    -- Simple heuristic veto example:
    -- Exclude if win odds are missing or zero
    CASE WHEN feat_odds_win_pre <= 0 THEN true ELSE false END AS veto_flag
FROM base
