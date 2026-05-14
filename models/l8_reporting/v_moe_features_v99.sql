{{ config(
    materialized='view',
    schema='api'
) }}

SELECT '20260426'::text AS race_date,
    '01'::text AS venue_code,
    '1'::text AS race_no,
    '01'::text AS umaban,
    0.0 AS feat_idm_zscore,
    0.0 AS feat_tt_bias_score,
    0.0 AS feat_tt_alpha_signal,
    0.0 AS feat_tt_confidence
WHERE 1 = 0
