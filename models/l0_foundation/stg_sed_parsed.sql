-- dbt/models/l0_foundation/stg_sed_parsed.sql
WITH source AS (
    SELECT * FROM {{ source('public', 'sed_parsed') }}
),
renamed AS (
    SELECT
        race_date,
        race_key,
        CAST(NULLIF(regexp_replace("馬番", '[^0-9]', '', 'g'), '') AS integer) AS umaban,
        CAST(NULLIF(regexp_replace("着順", '[^0-9]', '', 'g'), '') AS integer) AS actual_rank,
        CAST(NULLIF(regexp_replace("頭数", '[^0-9]', '', 'g'), '') AS integer) AS total_horses,

        -- 6馬券種の基礎目的変数 (1頭ごとの包含判定フラグ)
        CASE WHEN CAST(NULLIF(regexp_replace("着順", '[^0-9]', '', 'g'), '') AS integer) = 1 THEN 1 ELSE 0 END AS target_win,
        CASE WHEN CAST(NULLIF(regexp_replace("着順", '[^0-9]', '', 'g'), '') AS integer) <= 3 THEN 1 ELSE 0 END AS target_place,
        CASE WHEN CAST(NULLIF(regexp_replace("着順", '[^0-9]', '', 'g'), '') AS integer) <= 2 THEN 1 ELSE 0 END AS target_umatan,
        CASE WHEN CAST(NULLIF(regexp_replace("着順", '[^0-9]', '', 'g'), '') AS integer) <= 2 THEN 1 ELSE 0 END AS target_umaren,
        CASE WHEN CAST(NULLIF(regexp_replace("着順", '[^0-9]', '', 'g'), '') AS integer) <= 3 THEN 1 ELSE 0 END AS target_wide
        
        -- ※重要引継ぎ: WAKUREN (枠連)
        -- 枠連の判定にはレースごとの「出走頭数(total_horses)」による「枠番」の計算が必要です。
        -- このレイヤ(l0_foundation)では出走頭数を持たないため、
        -- 後段のレイヤ（l1_micro等）でレース情報と結合した上で `target_wakuren` を算出します。
    FROM source
    WHERE "馬番" IS NOT NULL AND "馬番" != ''
)
SELECT * FROM renamed
