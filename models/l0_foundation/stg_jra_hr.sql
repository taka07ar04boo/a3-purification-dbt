{{ config(materialized='view') }}

-- stg_jra_hr: JRA-VAN HR (払戻) 固定長パース
-- Phase 412: raw_data->>'raw_text' からSUBSTRING固定長パースに改修
-- HR record layout (total ~719 bytes):
--   Header: RT(2)+DK(1)+Date(8)=11 bytes
--   Race PK: kaisai_nen(4)+tsukihi(4)+keibajo(2)+kai(2)+nichime(2)+race_bango(2) = offset 11-26
--   Payout data: offset 27+ (複雑な繰返し構造)
--   - 単勝(27): umaban(2)+haraimodoshi(9)+ninki(3) x 3 = 42 bytes -> offset 27-68
--   - 複勝(69): umaban(2)+haraimodoshi(9)+ninki(3) x 3 = 42 bytes -> offset 69-110
--   - 馬連(111): kumi(4)+haraimodoshi(9)+ninki(3) x 3 = 48 bytes -> offset 111-158
--   ... (以降、馬単、ワイド、三連複、三連単)

WITH source AS (
    SELECT
        keibajo_code,
        kaisai_nen,
        kaisai_tsukihi,
        kaisai_kai,
        kaisai_nichime,
        race_bango,
        raw_data::json->>'raw_text' AS raw_text
    FROM {{ source('public', 'jvd_hr') }}
)
SELECT
    keibajo_code,
    kaisai_nen,
    kaisai_tsukihi,
    kaisai_kai,
    kaisai_nichime,
    race_bango,
    -- 単勝 1着 (offset 27: umaban 2 + haraimodoshi 9 + ninki 3)
    NULLIF(TRIM(SUBSTRING(raw_text FROM 28 FOR 2)), '')::INTEGER AS tansho_umaban_1,
    NULLIF(TRIM(SUBSTRING(raw_text FROM 30 FOR 9)), '')::INTEGER AS tansho_haraimodoshi_1,
    NULLIF(TRIM(SUBSTRING(raw_text FROM 39 FOR 3)), '')::INTEGER AS tansho_ninki_1,
    -- 複勝 1着 (offset 69)
    NULLIF(TRIM(SUBSTRING(raw_text FROM 70 FOR 2)), '')::INTEGER AS fukusho_umaban_1,
    NULLIF(TRIM(SUBSTRING(raw_text FROM 72 FOR 9)), '')::INTEGER AS fukusho_haraimodoshi_1,
    NULLIF(TRIM(SUBSTRING(raw_text FROM 81 FOR 3)), '')::INTEGER AS fukusho_ninki_1,
    -- 複勝 2着 (offset 83)
    NULLIF(TRIM(SUBSTRING(raw_text FROM 84 FOR 2)), '')::INTEGER AS fukusho_umaban_2,
    NULLIF(TRIM(SUBSTRING(raw_text FROM 86 FOR 9)), '')::INTEGER AS fukusho_haraimodoshi_2,
    NULLIF(TRIM(SUBSTRING(raw_text FROM 95 FOR 3)), '')::INTEGER AS fukusho_ninki_2,
    -- 複勝 3着 (offset 97)
    NULLIF(TRIM(SUBSTRING(raw_text FROM 98 FOR 2)), '')::INTEGER AS fukusho_umaban_3,
    NULLIF(TRIM(SUBSTRING(raw_text FROM 100 FOR 9)), '')::INTEGER AS fukusho_haraimodoshi_3,
    NULLIF(TRIM(SUBSTRING(raw_text FROM 109 FOR 3)), '')::INTEGER AS fukusho_ninki_3,
    -- raw_textも保持（後続の馬連/馬単/ワイド/三連複/三連単は構造が複雑なため）
    raw_text AS hr_raw_text
FROM source
