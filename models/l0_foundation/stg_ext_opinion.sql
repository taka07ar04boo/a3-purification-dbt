-- dbt/models/l0_foundation/stg_ext_opinion.sql
-- 外部からの無差別な予想データの正規化

{{ config(enabled=true) }}

WITH source AS (
    SELECT CAST(NULL AS VARCHAR) as id, CAST(NULL AS VARCHAR) as source_name, CAST(NULL AS VARCHAR) as target_at, CAST(NULL AS VARCHAR) as venue, CAST(NULL AS INT) as race, CAST(NULL AS INT) as horse_no, CAST(NULL AS NUMERIC) as score, CAST(NULL AS INT) as rank, CAST(NULL AS TIMESTAMP) as collected_at WHERE 1=0
),
renamed AS (
    SELECT
        id,
        source_name,
        target_at::text as target_at,
        venue,
        race::int as race,
        horse_no::int as horse_no,
        COALESCE(score, 0.0) as score,
        COALESCE(rank, 99) as rank,
        collected_at
    FROM source
)
SELECT * FROM renamed
