-- v_payouts_v100: 払戻集計ビュー (旧 api.v_payouts_v100 のdbt化)
-- payout > 0 の有効な払戻データを v91 と v39 から統合
{{ config(materialized='view', schema='api') }}

SELECT
    race_key,
    bet_type,
    umaban_1,
    umaban_2,
    payout,
    race_date,
    venue_code,
    race_no
FROM {{ source('api', 'a3_payouts_v91') }}
WHERE payout > 0

UNION ALL

SELECT
    race_key,
    bet_type,
    umaban_1,
    umaban_2,
    payout,
    race_date,
    venue_code,
    race_no
FROM {{ source('api', 'a3_payouts_v39') }}
WHERE payout > 0
