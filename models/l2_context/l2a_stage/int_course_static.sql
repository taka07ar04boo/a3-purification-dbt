-- dbt/models/l2_context/l2a_stage/int_course_static.sql

-- int_course_static

WITH source AS (
    -- PC-KEIBAのレースマスタなどから取得
    SELECT 
        kaisai_nen,
        kaisai_tsukihi,
        keibajo_code,
        race_bango,
        kyori::int as distance,
        track_code 
    FROM {{ source('public', 'jvd_ra') }}
    GROUP BY kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, kyori, track_code
)
SELECT DISTINCT * FROM source
