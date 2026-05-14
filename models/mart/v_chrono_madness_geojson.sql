{{ config(materialized='view', schema='chrono_archive') }}

SELECT json_build_object(
    'type', 'FeatureCollection',
    'features', COALESCE(json_agg(
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
                'intensity', intensity,
                'global_context', global_context,
                'event_category', event_category,
                'key_figures', key_figures,
                'impact_radius', impact_radius,
                'abstract_pattern', abstract_pattern
            )
        )
    ), '[]'::json)
) AS geojson_data
FROM chrono_archive.extracted_events
WHERE status_flag = 'GEOCODED' 
  AND latitude IS NOT NULL 
  AND longitude IS NOT NULL
