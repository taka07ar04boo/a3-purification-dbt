-- dbt/models/intermediate/int_jvan_phys_traits.sql
-- L26: JRA-VAN 物理特徴抽出 (重い結合の物理化)
-- L27/L25 の高速化のために、巨大テーブル(jvd_se, jvd_ra)の結合結果を一度に固定。

{{ config(materialized='table') }}

WITH base_entries AS (
    -- 最新の予測対象期間に限定してスキャン範囲を絞る（Temporal Pruning）
    SELECT DISTINCT target_at, venue, race FROM {{ ref('stg_ne_rankings') }}
),
jvan_traits AS (
    SELECT 
        se.kaisai_nen || '-' || substr(se.kaisai_tsukihi, 1, 2) || '-' || substr(se.kaisai_tsukihi, 3, 2) as target_at,
        se.keibajo_code as venue_code,
        se.race_bango as race,
        se.umaban as horse_no,
        se.blinker_shiyo_kubun as blinker,
        se.corner_4,
        se.barei,
        se.seibetsu_code,
        se.futan_juryo,
        se.bataiju,
        se.chokyoshi_code as chokyo_code,
        se.chokyoshimei_ryakusho as chokyo_name,
        se.kishu_code,
        se.kishumei_ryakusho as kishu_name,
        se.kohan_3f,
        se.tansho_odds,
        se.kyakushitsu_hantei,
        CAST(NULLIF(ra.kyori, '') AS integer) as distance
    FROM public.jvd_se se
    JOIN public.jvd_ra ra 
      ON se.kaisai_nen = ra.kaisai_nen 
     AND se.kaisai_tsukihi = ra.kaisai_tsukihi 
     AND se.keibajo_code = ra.keibajo_code 
     AND se.race_bango = ra.race_bango
    INNER JOIN base_entries be 
      ON (se.kaisai_nen || '-' || substr(se.kaisai_tsukihi, 1, 2) || '-' || substr(se.kaisai_tsukihi, 3, 2)) = be.target_at
     AND se.keibajo_code = be.venue
     AND se.race_bango = be.race::varchar -- 型不一致をここで解消
)
SELECT * FROM jvan_traits
