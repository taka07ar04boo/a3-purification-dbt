-- ================================================================
-- dbt モデル: stg_jrdb_sed (Phase 187 修正版)
-- sed テーブル (JRDB SEDレイアウト) からレース結果を標準化
-- ================================================================
{{ config(materialized='view', schema='api', enabled=true) }}

SELECT
    "場コード"                          AS venue_code,
    "年月日"                            AS race_date,
    "Ｒ"                               AS race_no,
    LPAD("馬番"::text, 2, '0')         AS umaban,
    "血統登録番号"                       AS horse_id,
    "馬名"                              AS horse_name,
    "着順"                              AS actual_rank,
    COALESCE(NULLIF(regexp_replace("着順"::text, '[^0-9]', '', 'g'), ''), '99')::int AS actual_rank_int,
    "確定単勝オッズ"::numeric / 10.0    AS win_odds_raw,
    "確定複勝オッズ下"::numeric / 10.0  AS place_odds_min,
    "確定単勝人気順位"                   AS pop_win,
    "距離"::int                         AS distance,
    "芝ダ障害コード"                     AS track_type,
    "馬場状態"                           AS track_cond,
    "条件"                               AS race_cond,
    "グレード"                           AS grade,
    "レース名"                           AS race_name,
    "頭数"::int                         AS total_horses,
    "タイム"                             AS finish_time,
    "後３Ｆタイム"                       AS rear3f,
    "コーナー順位４"                      AS corner4_rank,
    "馬体重"                             AS horse_weight,
    "馬体重増減"                          AS weight_diff,
    "異常区分"                            AS abnormal,
    "発走時間"                            AS start_time,
    -- 波乱指数: 人気6位以下で3着内
    CASE
        WHEN COALESCE(NULLIF(regexp_replace("確定単勝人気順位"::text, '[^0-9]', '', 'g'), ''), '0')::int >= 6
         AND COALESCE(NULLIF(regexp_replace("着順"::text, '[^0-9]', '', 'g'), ''), '99')::int <= 3
        THEN 1 ELSE 0
    END AS is_upset,
    -- 回収率計算用
    CASE
        WHEN COALESCE(NULLIF(regexp_replace("着順"::text, '[^0-9]', '', 'g'), ''), '99')::int = 1
        THEN "確定単勝オッズ"::numeric / 10.0
        ELSE 0
    END AS win_payout,
    -- レース種別 (regime 計算用)
    api.get_regime("芝ダ障害コード", NULLIF(regexp_replace("距離"::text, '[^0-9]', '', 'g'), '')::numeric) AS regime

FROM {{ source('public', 'sed_parsed') }}
WHERE "着順" IS NOT NULL
  AND "馬番" IS NOT NULL
