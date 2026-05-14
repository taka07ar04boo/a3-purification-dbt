{{ config(enabled=true) }}

-- models/l5_consilience/int_a3_consis_features.sql
-- 分解層: 基礎知能・環境要因の抽出 (高速化版)
-- L25-A: 物理テーブル L26 からの特徴抽出に移行。

WITH entries AS (
    SELECT * FROM {{ ref('stg_ne_rankings') }}
),
l1_points AS (
    SELECT * FROM {{ ref('int_horse_point_condition') }}
),
l2_dyn AS (
    SELECT * FROM {{ ref('int_race_dynamics') }}
),
l3_bias AS (
    SELECT * FROM {{ ref('int_day_track_bias') }}
),
jvan_traits AS (
    -- [ACCELERATION] 共通物理層(L26)から抽出。結合済みのデータを使用。
    SELECT * FROM {{ ref('int_jvan_phys_traits') }}
)
SELECT
    e.target_at, e.venue, e.race, e.horse_no, e.horse_name,
    REGEXP_REPLACE(e.horse_name, '[[:space:]　]', '', 'g') as clean_horse_name,
    js.distance, js.blinker, js.corner_4, js.futan_juryo, js.bataiju, js.kyakushitsu_hantei,
    -- [NEW] ゲノム結合用のキー
    js.chokyo_code,
    lp.immediate_form_score,
    rd.density_pressure,
    lb.inner_bias
FROM entries e
LEFT JOIN jvan_traits js ON e.target_at = js.target_at AND e.venue = js.venue_code AND e.race = js.race::INT AND e.horse_no = js.horse_no::INT
LEFT JOIN l1_points lp ON e.horse_name = lp.horse_name 
LEFT JOIN l2_dyn rd ON e.target_at = rd.target_at AND e.venue = rd.venue AND e.race = rd.race
LEFT JOIN l3_bias lb ON e.target_at = lb.target_at AND e.venue = lb.venue AND e.race = lb.race
