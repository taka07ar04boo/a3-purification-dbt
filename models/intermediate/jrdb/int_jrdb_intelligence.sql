{{ config(materialized='table') }}
WITH kyi AS (
    SELECT * FROM {{ ref('stg_jrdb_kyi') }}
),
tyb AS (
    -- 調教・直前: 直前指数
    SELECT jrdb_race_key, horse_number, idm_before, jockey_index_before, paddock_mark FROM {{ ref('stg_jrdb_tyb') }}
),
base AS (
    SELECT
        kyi.*,
        tyb.idm_before,
        tyb.jockey_index_before,
        tyb.paddock_mark
    FROM kyi
    LEFT JOIN tyb ON kyi.jrdb_race_key = tyb.jrdb_race_key AND kyi.horse_number = tyb.horse_number
)
SELECT
    *,
    -- 繋（つなぎ）による適性ロジックは列名不一致のため一時的に定数化。
    -- 後ほどAssetsレイヤーからの物理的特徴統合で修復する。
    1.0 AS joint_ground_aptitude,
    
    -- 調教と出馬表のコンシリエンス
    (COALESCE(idm, 0) + COALESCE(idm_before, 0)) / 2.0 AS idm_consilience_score
FROM base
