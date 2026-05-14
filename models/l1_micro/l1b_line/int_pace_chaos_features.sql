-- dbt/models/l1_micro/l1b_line/int_pace_chaos_features.sql

WITH source AS (
    SELECT
        jrdb_race_key AS race_key,
        horse_number AS umaban,
        COALESCE(idx_pace, 0) AS base_pace,
        COALESCE(pace_index, 0) AS feat_pace_advantage_score
    FROM {{ ref('stg_jrdb_kyi') }}
)
SELECT
    race_key,
    umaban,
    base_pace,
    feat_pace_advantage_score,
    -- Chaos index is stubbed in DB as 0. 
    -- True dynamic TDA entropy will be calculated in Python Thin Layer if needed,
    -- or kept as a static column here to ensure schema compatibility.
    0::numeric AS feat_tda_chaos_entropy
FROM source
