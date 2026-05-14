-- dbt/models/l0_foundation/stg_intelligence_audit.sql

{{ config(enabled=true) }}

WITH source AS (
    SELECT CAST(NULL AS VARCHAR) as id, CAST(NULL AS VARCHAR) as layer_name, CAST(NULL AS VARCHAR) as target_at, CAST(NULL AS VARCHAR) as venue, CAST(NULL AS INT) as race, CAST(NULL AS INT) as horse_no, CAST(NULL AS NUMERIC) as prediction_score, CAST(NULL AS VARCHAR) as actual_result, CAST(NULL AS NUMERIC) as error_margin, CAST(NULL AS TIMESTAMP) as audited_at WHERE 1=0
),
renamed AS (
    SELECT
        id,
        layer_name,
        target_at::text as target_at,
        venue,
        race::int as race,
        horse_no::int as horse_no,
        prediction_score,
        actual_result,
        error_margin,
        audited_at
    FROM source
)
SELECT * FROM renamed
