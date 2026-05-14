{{ config(
    materialized='view',
    schema='a3_purgatory'
) }}

SELECT
    (SELECT count(*) FROM {{ source('api', 'a3_h1_raw_chunks') }}) as total_chunks,
    (SELECT count(*) FROM {{ source('api', 'a3_h1_raw_chunks') }} WHERE embedding IS NOT NULL) as embedded_chunks,
    CASE
        WHEN (SELECT count(*) FROM {{ source('api', 'a3_h1_raw_chunks') }}) > 0 
        THEN round(
            (SELECT count(*)::numeric FROM {{ source('api', 'a3_h1_raw_chunks') }} WHERE embedding IS NOT NULL) 
            / (SELECT count(*)::numeric FROM {{ source('api', 'a3_h1_raw_chunks') }}) * 100, 
        2)
        ELSE 0
    END as completion_percentage,
    now() as last_checked_at
