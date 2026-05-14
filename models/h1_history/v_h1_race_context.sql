{{ config(
    materialized='view',
    schema='a3_purgatory'
) }}

SELECT 
    event_id,
    event_date,
    event_type,
    event_title,
    severity,
    location,
    confidence,
    impact_radius,
    metadata_json ->> 'global_context' AS global_context,
    metadata_json ->> 'abstract_pattern' AS abstract_pattern,
    metadata_json ->> 'key_figures' AS key_figures,
    CAST(metadata_json ->> 'infrastructure_collapse' AS boolean) AS infrastructure_collapse,
    CAST(metadata_json ->> 'rice_price_anomaly' AS boolean) AS rice_price_anomaly,
    metadata_json ->> 'collapse_signal' AS collapse_signal
FROM {{ source('api', 'a3_h1_events') }}
WHERE event_date IS NOT NULL
ORDER BY event_date DESC
