{{ config(materialized='view') }}

WITH geocoded_events AS (
    SELECT 
        id, 
        event_date, 
        event_location, 
        event_description, 
        intensity, 
        latitude, 
        longitude
    FROM {{ source('chrono_archive', 'extracted_events') }}
    WHERE status_flag = 'GEOCODED' 
      AND latitude IS NOT NULL 
      AND longitude IS NOT NULL
),
features AS (
    SELECT
        json_build_object(
            'type', 'Feature',
            'geometry', json_build_object(
                'type', 'Point',
                'coordinates', json_build_array(longitude, latitude)
            ),
            'properties', json_build_object(
                'id', id,
                'event_date', event_date,
                'event_location', event_location,
                'event_description', event_description,
                'intensity', intensity
            )
        ) AS feature
    FROM geocoded_events
)
SELECT
    json_build_object(
        'type', 'FeatureCollection',
        'features', json_agg(feature)
    ) AS geojson
FROM features
