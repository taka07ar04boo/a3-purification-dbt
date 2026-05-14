-- dbt/models/l2_context/l2b_physical/int_cushion_features.sql

WITH races AS (
    SELECT DISTINCT ON (jrdb_race_key)
        race_date,
        jrdb_race_key AS race_key,
        SUBSTRING(jrdb_race_key, 1, 2) AS venue_code
    FROM {{ ref('stg_jrdb_kyi') }}
    ORDER BY jrdb_race_key, race_date DESC
),
cushion AS (
    SELECT * FROM {{ ref('stg_jra_cushion') }}
),
cushion_features AS (
    SELECT
        r.race_date,
        r.race_key,
        r.venue_code,
        c.cushion_value,
        -- Python側と同等のデフォルト値 (9.5) と欠損補完
        COALESCE(c.cushion_value, 9.5)::numeric AS feat_env_cushion,
        COALESCE(c.cushion_value, 0)::numeric AS feat_jra_cushion_val
    FROM races r
    LEFT JOIN cushion c 
        ON r.race_date = c.target_at 
        AND r.venue_code = c.venue
)
SELECT * FROM cushion_features
