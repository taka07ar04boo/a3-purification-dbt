{{ config(
    materialized='table',
    schema='api',
    alias='a3_features_today_base',
    indexes=[
      {'columns': ['race_key', 'umaban']},
      {'columns': ['race_date']}
    ]
) }}

{% set exclude_cols = ['race_date','race_key','umaban','horse_id','horse_name','血統登録番号','馬名','score_idm','idx_ten','idx_agari','idx_pace','idx_pos','score_raw','info_score','jockey_score','info2_score','total_score','popular_rank','popular_class','pace_forecast','track_type','race_cond','race_symbol','distance','class_code','jockey_code','trainer_code','odds_win','pop_win','horse_weight','weight_diff'] %}

{% set kyi_relation = adapter.get_relation(database=target.database, schema='public', identifier='kyi_parsed') %}
{% if kyi_relation %}
    {% set all_cols = adapter.get_columns_in_relation(kyi_relation) %}
{% else %}
    {% set all_cols = [] %}
{% endif %}

WITH kyi AS (
    SELECT * FROM public.kyi_parsed
    WHERE race_date = '{{ var("target_date", "") }}'
),
ne_data AS (
    SELECT * FROM public.ne
),
ukc AS (
    SELECT DISTINCT ON ("血統登録番号") 
        "血統登録番号", 
        "父系統コード", 
        "母父系統コード" 
    FROM public.ukc_parsed_v50 
    ORDER BY "血統登録番号", race_date DESC
),
topo AS (
    SELECT * FROM {{ ref('int_topological_features') }}
),
veto AS (
    SELECT * FROM {{ ref('int_veto_flags') }}
),
cushion AS (
    SELECT * FROM {{ ref('int_cushion_features') }}
),
taketube AS (
    SELECT
        race_date,
        venue_code,
        race_no,
        AVG(inner_outer_bias) AS tt_inner_outer_bias,
        AVG(confidence) AS tt_confidence,
        AVG(bias_score) AS tt_bias_score,
        AVG(alpha_signal) AS tt_alpha_signal
    FROM api.a3_taketube_insights
    GROUP BY race_date, venue_code, race_no
),
tyb_agg AS (
    SELECT DISTINCT ON (tyb.race_date, tyb.race_key, tyb.umaban)
        tyb.race_date,
        tyb.race_key,
        tyb.umaban,
        CASE WHEN TRIM(tyb."総合指数") ~ '^[0-9.]+$' THEN TRIM(tyb."総合指数")::numeric ELSE 0::numeric END AS tyb_total_score,
        CASE WHEN TRIM(tyb."馬場状態コード") ~ '^[0-9.]+$' THEN TRIM(tyb."馬場状態コード")::numeric ELSE 0::numeric END AS tyb_track_cond,
        CASE WHEN TRIM(tyb."情報指数") ~ '^[-+]?[0-9]*\.?[0-9]+$' THEN TRIM(tyb."情報指数")::numeric ELSE 0::numeric END AS tyb_info_score
    FROM public.tyb_parsed tyb
    WHERE tyb.race_date = '{{ var("target_date", "") }}'
    ORDER BY tyb.race_date, tyb.race_key, tyb.umaban, tyb.id DESC
)

SELECT 
    k.race_date,
    k.race_key,
    k.umaban,
    k.class_code,
    -- base scores
    CASE WHEN TRIM(k.score_idm) ~ '^-?[0-9.]+$' THEN TRIM(k.score_idm)::numeric ELSE 0 END AS base_idm,
    CASE WHEN TRIM(k.idx_ten) ~ '^-?[0-9.]+$' THEN TRIM(k.idx_ten)::numeric ELSE 0 END AS base_ten,
    CASE WHEN TRIM(k.idx_agari) ~ '^-?[0-9.]+$' THEN TRIM(k.idx_agari)::numeric ELSE 0 END AS base_agari,
    CASE WHEN TRIM(k.idx_pace) ~ '^-?[0-9.]+$' THEN TRIM(k.idx_pace)::numeric ELSE 0 END AS base_pace,
    CASE WHEN TRIM(k."激走指数") ~ '^-?[0-9.]+$' THEN TRIM(k."激走指数")::numeric ELSE 0 END AS base_gekisou,
    -- ================================================================
    -- v92 deployment package feature mapping (58 features)
    -- These are the exact column names the L1/L3 models expect
    -- ================================================================
    -- Core indices
    CASE WHEN TRIM(k."枠番") ~ '^[0-9]+$' THEN TRIM(k."枠番")::numeric ELSE 0 END AS feat_gate_no,
    CASE WHEN TRIM(k.score_idm) ~ '^-?[0-9.]+$' THEN TRIM(k.score_idm)::numeric ELSE 0 END AS feat_score_idm,
    CASE WHEN TRIM(k.idx_ten) ~ '^-?[0-9.]+$' THEN TRIM(k.idx_ten)::numeric ELSE 0 END AS feat_idx_ten,
    CASE WHEN TRIM(k.idx_agari) ~ '^-?[0-9.]+$' THEN TRIM(k.idx_agari)::numeric ELSE 0 END AS feat_idx_agari,
    CASE WHEN TRIM(k.odds_win) ~ '^-?[0-9.]+$' THEN TRIM(k.odds_win)::numeric ELSE 0 END AS feat_odds_win_pre,
    CASE WHEN TRIM(k.pop_win) ~ '^-?[0-9.]+$' THEN TRIM(k.pop_win)::numeric ELSE 0 END AS feat_pop_win_pre,
    CASE WHEN TRIM(k."クラスコード") ~ '^[0-9]+$' THEN TRIM(k."クラスコード")::numeric ELSE 0 END AS feat_class_code_num,
    -- Aptitude and system codes
    CASE WHEN TRIM(k."激走指数") ~ '^-?[0-9.]+$' THEN TRIM(k."激走指数")::numeric ELSE 0 END AS feat_gekiso_index,
    CASE WHEN TRIM(k."距離適性") ~ '^[0-9]+$' THEN TRIM(k."距離適性")::numeric ELSE 0 END AS feat_dist_aptitude,
    CASE WHEN TRIM(k."距離適性２") ~ '^[0-9]+$' THEN TRIM(k."距離適性２")::numeric ELSE 0 END AS feat_dist_aptitude2,
    CASE WHEN TRIM(k."厩舎評価コード") ~ '^[0-9]+$' THEN TRIM(k."厩舎評価コード")::numeric ELSE 0 END AS feat_stable_eval_code,
    CASE WHEN TRIM(k."調教矢印コード") ~ '^[0-9]+$' THEN TRIM(k."調教矢印コード")::numeric ELSE 0 END AS feat_training_arrow_code,
    CASE WHEN TRIM(k."芝適性コード") ~ '^[0-9]+$' THEN TRIM(k."芝適性コード")::numeric ELSE 0 END AS feat_grass_aptitude_code,
    CASE WHEN TRIM(k."ダ適性コード") ~ '^[0-9]+$' THEN TRIM(k."ダ適性コード")::numeric ELSE 0 END AS feat_dirt_aptitude_code,
    CASE WHEN TRIM(k."脚質") ~ '^[0-9]+$' THEN TRIM(k."脚質")::numeric ELSE 0 END AS feat_running_style_code,
    CASE WHEN TRIM(k."重適正コード") ~ '^[0-9]+$' THEN TRIM(k."重適正コード")::numeric ELSE 0 END AS feat_mud_aptitude_code,
    CASE WHEN TRIM(k."走法") ~ '^[0-9]+$' THEN TRIM(k."走法")::numeric ELSE 0 END AS feat_running_form_code,
    CASE WHEN TRIM(k."体型") ~ '^[0-9]+$' THEN TRIM(k."体型")::numeric ELSE 0 END AS feat_body_type_code,
    CASE WHEN TRIM(k."ローテーション") ~ '^[0-9]+$' THEN TRIM(k."ローテーション")::numeric ELSE 0 END AS feat_rotation_days,
    CASE WHEN TRIM(k."万券指数") ~ '^-?[0-9.]+$' THEN TRIM(k."万券指数")::numeric ELSE 0 END AS feat_longshot_index,
    CASE WHEN TRIM(k."馬スタート指数") ~ '^-?[0-9.]+$' THEN TRIM(k."馬スタート指数")::numeric ELSE 0 END AS feat_start_index,
    CASE WHEN TRIM(k."馬出遅率") ~ '^-?[0-9.]+$' THEN TRIM(k."馬出遅率")::numeric ELSE 0 END AS feat_late_start_rate,
    CASE WHEN TRIM(k."人気指数") ~ '^-?[0-9.]+$' THEN TRIM(k."人気指数")::numeric ELSE 0 END AS feat_popular_index,
    CASE WHEN TRIM(k."騎手期待連対率") ~ '^-?[0-9.]+$' THEN TRIM(k."騎手期待連対率")::numeric ELSE 0 END AS feat_jockey_exp_rentai,
    CASE WHEN TRIM(k."騎手期待単勝率") ~ '^-?[0-9.]+$' THEN TRIM(k."騎手期待単勝率")::numeric ELSE 0 END AS feat_jockey_exp_win,
    CASE WHEN TRIM(k."騎手期待３着内率") ~ '^-?[0-9.]+$' THEN TRIM(k."騎手期待３着内率")::numeric ELSE 0 END AS feat_jockey_exp_top3,
    -- Sire/BMS system codes from ukc (G-09 FIX: numeric型 + 血統登録番号JOIN)
    COALESCE(NULLIF(TRIM(u."父系統コード"), ''), '0')::numeric AS feat_sire_system_code,
    COALESCE(NULLIF(TRIM(u."母父系統コード"), ''), '0')::numeric AS feat_bms_system_code,
    -- Pace advantage
    CASE WHEN TRIM(k."ペース指数") ~ '^-?[0-9.]+$' THEN TRIM(k."ペース指数")::numeric ELSE 0 END AS feat_pace_advantage_score,
    -- Cross features (computed from base features)
    CASE WHEN TRIM(k."激走指数") ~ '^-?[0-9.]+$' AND TRIM(k.distance) ~ '^[0-9]+$'
        THEN TRIM(k."激走指数")::numeric * TRIM(k.distance)::numeric / 1000.0 ELSE 0 END AS feat_gekiso_dist_cross,
    CASE WHEN TRIM(k."調教指数") ~ '^-?[0-9.]+$' AND TRIM(k."厩舎指数") ~ '^-?[0-9.]+$'
        THEN TRIM(k."調教指数")::numeric * TRIM(k."厩舎指数")::numeric / 100.0 ELSE 0 END AS feat_training_stable_cross,
    -- Track fit signal: grass aptitude for turf, dirt aptitude for dirt
    CASE WHEN TRIM(k.track_type) = '1' THEN
        CASE WHEN TRIM(k."芝適性コード") ~ '^[0-9]+$' THEN TRIM(k."芝適性コード")::numeric ELSE 0 END
    ELSE
        CASE WHEN TRIM(k."ダ適性コード") ~ '^[0-9]+$' THEN TRIM(k."ダ適性コード")::numeric ELSE 0 END
    END AS feat_track_fit_signal,
    -- Low popularity but high IDM signal
    CASE WHEN TRIM(k.pop_win) ~ '^[0-9]+$' AND TRIM(k.score_idm) ~ '^-?[0-9.]+$'
        AND TRIM(k.pop_win)::numeric > 5 AND TRIM(k.score_idm)::numeric > 50
        THEN 1 ELSE 0 END AS feat_low_pop_high_idm,

    -- env
    COALESCE(cushion.feat_env_cushion, 9.5)::numeric AS feat_env_cushion,
    0::numeric AS feat_env_moisture,
    -- intent
    CASE WHEN TRIM(k."調教指数") ~ '^-?[0-9.]+$' THEN TRIM(k."調教指数")::numeric ELSE 0 END AS feat_intent_training_idx,
    CASE WHEN TRIM(k."厩舎指数") ~ '^-?[0-9.]+$' THEN TRIM(k."厩舎指数")::numeric ELSE 0 END AS feat_intent_stable_idx,
    CASE WHEN TRIM(k."騎手期待連対率") ~ '^-?[0-9.]+$' THEN TRIM(k."騎手期待連対率")::numeric ELSE 0 END AS feat_intent_jockey_exp_win_rate,
    TRIM(k."調教矢印コード") AS cat_intent_training_arrow,
    TRIM(k."蹄コード") AS cat_genome_hoof_code,
    TRIM(k."重適正コード") AS cat_genome_heavy_track_code,
    TRIM(k."脚質") AS cat_genome_run_style,
    ''::text AS cat_taketube_pace_type,
    0::numeric AS feat_taketube_inner_bias,
    0::numeric AS feat_taketube_forgetfulness,
    0::numeric AS feat_jockey_agent_score,
    0::numeric AS feat_bias_performance_score,
    COALESCE(ne."fu2", 0)::numeric AS feat_fu2,
    COALESCE(ne."Ｆ指数", 0)::numeric AS feat_f_index,
    -- topo (race-level stats from dbt layer)
    COALESCE(topo.feat_env_std_raw_score_idm, 0)::numeric AS feat_env_std_raw_score_idm,
    COALESCE(topo.feat_env_mean_raw_score_idm, 0)::numeric AS feat_env_mean_raw_score_idm,
    COALESCE(topo.feat_env_std_raw_idx_ten, 0)::numeric AS feat_env_std_raw_idx_ten,
    COALESCE(topo.feat_env_mean_raw_idx_ten, 0)::numeric AS feat_env_mean_raw_idx_ten,
    COALESCE(topo.feat_env_std_raw_idx_agari, 0)::numeric AS feat_env_std_raw_idx_agari,
    COALESCE(topo.feat_env_mean_raw_idx_agari, 0)::numeric AS feat_env_mean_raw_idx_agari,
    COALESCE(topo.feat_env_std_idx_pace, 0)::numeric AS feat_env_std_idx_pace,
    COALESCE(topo.feat_env_mean_idx_pace, 0)::numeric AS feat_env_mean_idx_pace,
    0::numeric AS feat_env_std_idx_race_p, -- to be implemented if exists in source
    0::numeric AS feat_env_mean_idx_race_p, -- to be implemented if exists in source
    COALESCE(topo.feat_chaos_odds_entropy, 0)::numeric AS feat_chaos_odds_entropy,
    COALESCE(topo.feat_chaos_idm_spread, 0)::numeric AS feat_chaos_idm_spread,
    COALESCE(topo.feat_field_size, 0)::numeric AS feat_field_size,
    
    COALESCE(veto.veto_flag, false)::boolean AS veto_flag,
    
    (CASE WHEN TRIM(k.score_idm) ~ '^-?[0-9.]+$' THEN TRIM(k.score_idm)::numeric ELSE 0 END) - COALESCE(topo.feat_env_mean_raw_score_idm, 0)::numeric AS feat_topo_idm_diff,
    COALESCE(topo.feat_env_std_raw_score_idm, 0)::numeric AS feat_topo_idm_std,
    0::numeric AS feat_topo_idm_mad,
    (CASE WHEN TRIM(k.idx_pace) ~ '^-?[0-9.]+$' THEN TRIM(k.idx_pace)::numeric ELSE 0 END) - COALESCE(topo.feat_env_mean_idx_pace, 0)::numeric AS feat_topo_pace_diff,
    COALESCE(topo.feat_env_std_idx_pace, 0)::numeric AS feat_topo_pace_std,
    0::numeric AS feat_topo_pace_mad,
    0::numeric AS feat_class_gap_idm,
    
    COALESCE(t.tyb_total_score, 0)::numeric AS feat_tyb_total_score,
    COALESCE(t.tyb_track_cond, 0)::numeric AS feat_tyb_track_cond,
    
    COALESCE(topo.feat_env_mean_tyb_info, 0)::numeric AS feat_topo_info_mean,
    COALESCE(topo.feat_env_std_tyb_info, 0)::numeric AS feat_topo_info_std,
    COALESCE(t.tyb_info_score, 0)::numeric - COALESCE(topo.feat_env_mean_tyb_info, 0)::numeric AS feat_topo_info_diff,

    COALESCE(topo.feat_env_mean_raw_score_idm, 0)::numeric AS feat_topo_total_mean,
    COALESCE(topo.feat_env_std_raw_score_idm, 0)::numeric AS feat_topo_total_std,
    (CASE WHEN TRIM(k.score_idm) ~ '^-?[0-9.]+$' THEN TRIM(k.score_idm)::numeric ELSE 0 END) - COALESCE(topo.feat_env_mean_raw_score_idm, 0)::numeric AS feat_topo_total_diff,
    
    {% for col in all_cols %}
        {% if col.name not in exclude_cols %}
            {% if col.data_type in ['text', 'character varying', 'varchar'] %}
                CASE WHEN TRIM(k."{{ col.name }}") ~ '^-?[0-9]+(\.[0-9]*)?$' 
                     THEN TRIM(k."{{ col.name }}")::numeric 
                     ELSE 0 END AS "feat_kyi_{% if col.name.endswith('_2') %}予備{% elif col.name.endswith('_3') %}予備{% elif col.name.endswith('_4') %}予備{% else %}{{ col.name }}{% endif %}",
            {% else %}
                k."{{ col.name }}" AS "feat_kyi_{% if col.name.endswith('_2') %}予備{% elif col.name.endswith('_3') %}予備{% elif col.name.endswith('_4') %}予備{% else %}{{ col.name }}{% endif %}",
            {% endif %}
        {% endif %}
    {% endfor %}

    -- JRA cushion / taketube etc
    COALESCE(cushion.feat_jra_cushion_val, 0)::numeric AS feat_jra_cushion_val,
    COALESCE(taketube.tt_inner_outer_bias, 0)::numeric AS feat_tt_inner_outer_bias,
    0::numeric AS feat_tt_avg_forgetfulness,
    COALESCE(taketube.tt_confidence, 0)::numeric AS feat_tt_confidence,
    COALESCE(taketube.tt_bias_score, 0)::numeric AS feat_tt_bias_score,
    COALESCE(taketube.tt_alpha_signal, 0)::numeric AS feat_tt_alpha_signal,
    ''::text AS feat_tt_pace_type,
    0::numeric AS feat_tt_favorable_position,
    0::numeric AS feat_tt_video_count,
    0::numeric AS feat_tt_eval_rank,
    0::numeric AS feat_tt_is_blinker_boost,
    0::numeric AS feat_tt_is_local_yari,
    0::numeric AS feat_tt_class_potential_gap,
    0::numeric AS feat_cho_paddock_score,
    0::numeric AS feat_cho_weight_change,
    0::numeric AS feat_cho_vibe_score,
    -- extra required by model
    0::numeric AS feat_gnn_sire_emb,
    0::numeric AS feat_gnn_topology_emb,
    0::numeric AS feat_causal_uplift_jockey,
    0::numeric AS feat_causal_uplift_equipment,
    0::numeric AS feat_tda_chaos_entropy,
    0::integer AS feat_race_class_cat,
    -- realtime features (3分ループでUPDATEされる)
    0::numeric AS feat_realtime_win_odds,
    0::numeric AS feat_realtime_weight,
    0::numeric AS feat_realtime_weight_diff,
    0::numeric AS feat_realtime_paddock,
    0::numeric AS feat_realtime_condition,
    0::numeric AS feat_realtime_mood,
    0::numeric AS feat_realtime_final_mark,
    0::numeric AS feat_bias_inner_outer,
    0::numeric AS feat_bias_run_style,
    0::numeric AS feat_bias_track_change,
    -- sire/bms from ukc
    COALESCE(u."父系統コード", '')::text AS feat_sire_cat_raw,
    COALESCE(u."母父系統コード", '')::text AS feat_bms_cat_raw,
    
    -- missing legacy features
    0::numeric AS "feat_kyi_特定情報◎",
    0::numeric AS "feat_kyi_特定情報○",
    0::numeric AS "feat_kyi_特定情報▲",
    0::numeric AS "feat_kyi_特定情報△",
    0::numeric AS "feat_kyi_特定情報×",
    0::numeric AS "feat_kyi_総合情報◎",
    0::numeric AS "feat_kyi_総合情報○",
    0::numeric AS "feat_kyi_総合情報▲",
    0::numeric AS "feat_kyi_総合情報△",
    0::numeric AS "feat_kyi_総合情報×",
    0::numeric AS "feat_kyi_予備_2",
    0::numeric AS "feat_kyi_予備_3",
    0::numeric AS "feat_kyi_予備_4",
    0::numeric AS "feat_kyi_ls指数順位",
    
    RANK() OVER (PARTITION BY k.race_key ORDER BY CASE WHEN TRIM(k.score_idm) ~ '^-?[0-9.]+$' THEN TRIM(k.score_idm)::numeric ELSE 0 END DESC) AS idm_rank

FROM kyi k
LEFT JOIN ne_data ne 
  ON ne."年月日" = CASE WHEN length(k.race_date) = 8 THEN substring(k.race_date,1,4)||'/'||substring(k.race_date,5,2)||'/'||substring(k.race_date,7,2) ELSE k.race_date END
  AND ne."馬番" = k.umaban
  AND ne."Ｒ" = TRIM(k."Ｒ")
  AND ne."場所" = TRIM(k."場コード")
LEFT JOIN ukc u 
  ON u."血統登録番号" = k."血統登録番号"
LEFT JOIN topo 
  ON topo.race_key = k.race_key
LEFT JOIN veto 
  ON veto.race_key = k.race_key AND veto.umaban = k.umaban
LEFT JOIN cushion
  ON cushion.race_key = k.race_key
LEFT JOIN taketube
  ON taketube.race_date = CASE WHEN length(k.race_date) = 8 THEN substring(k.race_date,1,4)||'-'||substring(k.race_date,5,2)||'-'||substring(k.race_date,7,2) ELSE k.race_date END::date
  AND taketube.venue_code = SUBSTRING(k.race_key, 1, 2)
  AND taketube.race_no = TRIM(k."Ｒ")::integer
LEFT JOIN tyb_agg t
  ON t.race_key = k.race_key AND t.umaban::integer = k.umaban::integer
