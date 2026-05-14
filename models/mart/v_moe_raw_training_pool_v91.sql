{{ config(
    materialized='view',
    schema='api',
    enabled=false
) }}

SELECT 
    m.race_date,
    m.venue_code,
    m.race_no,
    m.umaban,
    m.horse_id,
    m.horse_name,
    m.jockey_code,
    m.trainer_code,
    m.is_hurdle,
    m.distance_m,
    m.total_horses,
    m.surface_type,
    
    -- TARGET VARIABLES (Not Features)
    m.actual_rank,
    m.is_winner,
    m.is_top3,
    m.is_top5,

    -- PURE PREDICTIVE FEATURES (Prefixed with feat_)
    m.idm_zscore AS feat_idm_zscore,
    m.ten_zscore AS feat_ten_zscore,
    m.agari_zscore AS feat_agari_zscore,
    m.race_std_idm AS feat_race_std_idm,
    m.race_std_ten AS feat_race_std_ten,
    m.race_std_agari AS feat_race_std_agari,
    m.odds_win_pre AS feat_odds_win_pre,
    m.pop_win_pre AS feat_pop_win_pre,
    m.low_pop_high_idm AS feat_low_pop_high_idm,
    m.pace_advantage_score AS feat_pace_advantage_score,
    m.track_fit_signal AS feat_track_fit_signal,
    m.jvd_cushion_val AS feat_jvd_cushion_val,
    m.rotation_days AS feat_rotation_days,
    m.jockey_exp_win AS feat_jockey_exp_win,
    m.jockey_exp_top3 AS feat_jockey_exp_top3,
    m.tt_bias_score AS feat_tt_bias_score,
    m.tt_alpha_signal AS feat_tt_alpha_signal,
    m.tt_confidence AS feat_tt_confidence,
    m.start_index AS feat_start_index,
    m.popular_index AS feat_popular_index,
    m.longshot_index AS feat_longshot_index,
    m.gekiso_index AS feat_gekiso_index

FROM {{ ref('v_moe_raw_training_pool_v90') }} m
