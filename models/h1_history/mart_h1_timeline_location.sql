{{ config(
    materialized='view'
) }}

SELECT
    event_location_modern AS location_name,
    normalized_date,
    event_date,
    key_figures,
    event_category,
    intensity,
    event_description_modern,
    id AS event_id
FROM {{ source('chrono_archive', 'extracted_events') }}
WHERE event_location_modern IS NOT NULL AND event_location_modern != '' AND event_location_modern != 'Unknown'
ORDER BY event_location_modern, normalized_date ASC NULLS LAST
