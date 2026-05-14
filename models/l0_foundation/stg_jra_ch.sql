{{ config(materialized='view') }}

-- stg_jra_ch: JRA-VAN CH (調教師) 固定長パース
-- Phase 412: raw_data->>'raw_text' からSUBSTRING固定長パースに改修
-- CH record layout:
--   Header: RT(2)+DK(1)+Date(8)=11 bytes
--   chokyoshi_code(5) at offset 11
--   chokyoshi_mei(34) at offset 16 (推定)
--   chokyoshi_meiryu(20) at offset 50 (推定)

WITH source AS (
    SELECT
        chokyoshi_code,
        raw_data::json->>'raw_text' AS raw_text
    FROM {{ source('public', 'jvd_ch') }}
)
SELECT
    chokyoshi_code,
    NULLIF(TRIM(SUBSTRING(raw_text FROM 17 FOR 34)), '') AS chokyoshi_mei,
    NULLIF(TRIM(SUBSTRING(raw_text FROM 51 FOR 20)), '') AS chokyoshi_meiryu,
    NULLIF(TRIM(SUBSTRING(raw_text FROM 71 FOR 8)), '') AS seinengappi,
    raw_text AS ch_raw_text
FROM source
