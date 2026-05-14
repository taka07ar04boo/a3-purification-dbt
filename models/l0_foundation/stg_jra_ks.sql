{{ config(materialized='view') }}

-- stg_jra_ks: JRA-VAN KS (騎手) 固定長パース
-- Phase 412: raw_data->>'raw_text' からSUBSTRING固定長パースに改修
-- KS record layout:
--   Header: RT(2)+DK(1)+Date(8)=11 bytes
--   kishu_code(5) at offset 11
--   kishu_mei(34) at offset 16 (推定)
--   kishu_meiryu(20) at offset 50 (推定)

WITH source AS (
    SELECT
        kishu_code,
        raw_data::json->>'raw_text' AS raw_text
    FROM {{ source('public', 'jvd_ks') }}
)
SELECT
    kishu_code,
    NULLIF(TRIM(SUBSTRING(raw_text FROM 17 FOR 34)), '') AS kishu_mei,
    NULLIF(TRIM(SUBSTRING(raw_text FROM 51 FOR 20)), '') AS kishu_meiryu,
    NULLIF(TRIM(SUBSTRING(raw_text FROM 71 FOR 8)), '') AS seinengappi,
    raw_text AS ks_raw_text
FROM source
