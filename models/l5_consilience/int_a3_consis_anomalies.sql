{{ config(enabled=true) }}

-- models/l5_consilience/int_a3_consis_anomalies.sql
-- 分解層: コンシリエンス異常検知 (高速化版)・アノマリーの抽出
-- L25-B: 並列化のため特殊ロジックを分離。

WITH entries AS (
    SELECT * FROM {{ ref('stg_ne_rankings') }}
),
l1_history AS (
    SELECT * FROM {{ ref('int_horse_race_history') }}
),
jrdb_assets AS (
    SELECT 
        CAST(NULL AS VARCHAR) as horse_id,
        CAST(NULL AS VARCHAR) as "体型",
        CAST(NULL AS VARCHAR) as "蹄コード",
        CAST(NULL AS VARCHAR) as "距離適性"
),
l1_fixed AS (
    SELECT * FROM {{ ref('int_horse_fixed') }}
)
SELECT
    e.target_at, e.venue, e.race, e.horse_no, e.horse_name,
    hf.horse_id,
    hf.clean_horse_name,
    hh.prev_rank, hh.prev_pop, hh.prev_c4_pos, hh.prev_weight, hh.prev_distance, hh.prev_race_date,
    hh.win_rate, hh.place_rate, hh.real_koso_rate,
    ja."体型" as phys_type,
    ja."蹄コード" as hoof_code
FROM entries e
LEFT JOIN l1_fixed hf ON REGEXP_REPLACE(e.horse_name, '[[:space:]　]', '', 'g') = hf.clean_horse_name
LEFT JOIN l1_history hh ON hf.horse_id = hh.horse_id
LEFT JOIN jrdb_assets ja ON hf.horse_id = ja.horse_id
