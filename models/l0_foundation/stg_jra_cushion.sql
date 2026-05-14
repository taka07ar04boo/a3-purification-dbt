-- dbt/models/l0_foundation/stg_jra_cushion.sql

WITH source AS (
    SELECT * FROM {{ source('public', 'a3_jra_cushion') }}
),
renamed AS (
    SELECT DISTINCT ON (target_date::text, venue)
        target_date::text as target_at,
        venue,
        cushion_val::float as cushion_value,
        updated_at
    FROM source
    ORDER BY target_date::text, venue, updated_at DESC
)
SELECT * FROM renamed
