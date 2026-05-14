{{ config(
    materialized='view'
) }}

WITH stats AS (
    SELECT
        COUNT(*) AS total_events,
        COUNT(event_description_modern) AS modern_translated,
        COUNT(event_description_layman) AS layman_translated,
        COUNT(ai_interpretation) AS ai_translated
    FROM chrono_archive.extracted_events
)
SELECT
    total_events,
    modern_translated,
    layman_translated,
    ai_translated,
    ROUND((modern_translated::NUMERIC / NULLIF(total_events, 0)) * 100, 2) AS modern_coverage_pct,
    ROUND((layman_translated::NUMERIC / NULLIF(total_events, 0)) * 100, 2) AS layman_coverage_pct,
    ROUND((ai_translated::NUMERIC / NULLIF(total_events, 0)) * 100, 2) AS ai_coverage_pct,
    CASE 
        WHEN modern_translated = total_events 
         AND layman_translated = total_events 
         AND ai_translated = total_events THEN 'COMPLETED'
        ELSE 'IN_PROGRESS'
    END AS translation_status
FROM stats
