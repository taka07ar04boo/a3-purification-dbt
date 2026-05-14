{{ config(
    materialized='view',
    schema='a3_purgatory'
) }}

WITH categorized AS (
    SELECT 
        event_category,
        abstract_pattern,
        metadata_json ->> 'event_location_raw' AS event_location,
        location AS event_location_modern,
        intensity,
        CAST(impact_radius AS numeric) AS impact_radius,
        global_context,
        key_figures,
        CASE
            WHEN embedding_vector IS NOT NULL THEN true
            ELSE false
        END AS has_embedding
    FROM {{ source('api', 'a3_h1_events') }}
    WHERE abstract_pattern IS NOT NULL
)
SELECT 
    event_category,
    count(*) AS pattern_count,
    round(avg(intensity), 2) AS avg_intensity,
    round(avg(impact_radius), 2) AS avg_impact_radius,
    count(DISTINCT abstract_pattern) AS unique_patterns,
    count(DISTINCT event_location_modern) FILTER (WHERE event_location_modern IS NOT NULL) AS distinct_modern_locations
FROM categorized
GROUP BY event_category
ORDER BY count(*) DESC
