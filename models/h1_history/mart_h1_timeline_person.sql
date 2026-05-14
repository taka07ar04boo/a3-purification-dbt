{{ config(
    materialized='view'
) }}

WITH unnested_persons AS (
    SELECT
        id AS event_id,
        TRIM(unnest(string_to_array(key_figures, ','))) AS person_name,
        normalized_date,
        event_date,
        event_location_modern,
        event_description_modern,
        intensity,
        impact_radius,
        event_category
    FROM {{ source('chrono_archive', 'extracted_events') }}
    WHERE key_figures IS NOT NULL AND key_figures != ''
)

SELECT
    person_name,
    normalized_date,
    event_date,
    event_location_modern,
    event_category,
    intensity,
    event_description_modern,
    event_id
FROM unnested_persons
WHERE person_name != '' AND person_name != 'Unknown'
ORDER BY person_name, normalized_date ASC NULLS LAST
