-- v_historical_resonance_signal: H1パターン集約ビュー (旧 public.v_historical_resonance_signal のdbt化)
-- H1歴史イベントから共鳴シグナルを検出
{{ config(materialized='view') }}

SELECT
    event_id AS id,
    metadata_json ->> 'event_date_raw' AS event_date,
    event_category,
    historical_panic_index,
    infrastructure_collapse_flag,
    rice_price_anomaly,
    collapse_signal_detail,
    abstract_pattern,
    CASE
        WHEN historical_panic_index > 0.7
            OR infrastructure_collapse_flag = true
            OR rice_price_anomaly = true
        THEN 'CRITICAL_RESONANCE'
        ELSE 'NORMAL'
    END AS resonance_level
FROM {{ source('api', 'a3_h1_events') }}
WHERE historical_panic_index > 0.0
    OR infrastructure_collapse_flag = true
    OR rice_price_anomaly = true
