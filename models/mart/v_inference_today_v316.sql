{{ config(
    materialized='view',
    schema='api'
) }}

-- v316: Final inference view with race-level statistics computed via WINDOW functions
-- Phase 401: Stub resolution for 11 of 12 hardcoded-zero columns
-- Phase 405: CRITICAL fix — Chaos detection thresholds corrected (previously 100% disabled)
-- Phase 421: feat_heritage_golden_match RESOLVED — inline CASE replaces mv_heritage_golden_rules dependency

WITH base AS (
    SELECT v.*,
        -- =====================================================
        -- WINDOW FUNCTIONS: Race-level statistics (per race)
        -- These replace the hardcoded 0 stubs from Phase 391
        -- =====================================================
        -- Race-level STD (standard deviation of core indices within race)
        COALESCE(v.race_std_idm, STDDEV_POP(v.score_idm) OVER (PARTITION BY v.race_date, v.venue_code, v.race_no))::numeric
            AS w_race_std_score_idm,
        COALESCE(v.race_std_ten, STDDEV_POP(v.idx_ten) OVER (PARTITION BY v.race_date, v.venue_code, v.race_no))::numeric
            AS w_race_std_idx_ten,
        COALESCE(v.race_std_agari, STDDEV_POP(v.idx_agari) OVER (PARTITION BY v.race_date, v.venue_code, v.race_no))::numeric
            AS w_race_std_idx_agari,

        -- Race-level MEAN (average of core indices within race)
        AVG(v.score_idm) OVER (PARTITION BY v.race_date, v.venue_code, v.race_no)::numeric
            AS w_race_mean_score_idm,
        AVG(v.idx_ten) OVER (PARTITION BY v.race_date, v.venue_code, v.race_no)::numeric
            AS w_race_mean_idx_ten,
        AVG(v.idx_agari) OVER (PARTITION BY v.race_date, v.venue_code, v.race_no)::numeric
            AS w_race_mean_idx_agari,

        -- Mean Absolute Deviation (MAD proxy): |value - race_mean|
        -- Uses AVG-based deviation since PERCENTILE_CONT is not available as WINDOW function in PG
        ABS(v.score_idm - AVG(v.score_idm) OVER (PARTITION BY v.race_date, v.venue_code, v.race_no))::numeric
            AS w_score_idm_mad,
        ABS(v.idx_ten - AVG(v.idx_ten) OVER (PARTITION BY v.race_date, v.venue_code, v.race_no))::numeric
            AS w_idx_ten_mad,
        ABS(v.idx_agari - AVG(v.idx_agari) OVER (PARTITION BY v.race_date, v.venue_code, v.race_no))::numeric
            AS w_idx_agari_mad,

        -- Topological chaos indicator: entropy of pace advantage scores within race
        -- High variance in pace_advantage_score indicates chaotic race dynamics
        COALESCE(STDDEV_POP(v.pace_advantage_score) OVER (PARTITION BY v.race_date, v.venue_code, v.race_no), 0)::numeric
            AS w_topological_chaos,

        -- MAD pace resistance: absolute deviation of pace_advantage_score from race mean
        ABS(v.pace_advantage_score - AVG(v.pace_advantage_score) OVER (PARTITION BY v.race_date, v.venue_code, v.race_no))::numeric
            AS w_mad_pace_resistance

    FROM {{ ref('v_inference_today_v56') }} v
)

SELECT base.race_date,
     base.venue_code,
     base.race_no,
     base.umaban,
     base.horse_id,
     base.horse_name,
     base.jockey_code,
     base.trainer_code,
     base.gate_no,
     base.score_idm,
     base.idx_ten,
     base.idx_agari,
     base.odds_win_pre,
     base.pop_win_pre,
     base.distance_m,
     base.total_horses,
     base.class_code_num,
     base.is_hurdle,
     base.surface_type,
     base.idm_zscore,
     base.ten_zscore,
     base.agari_zscore,
     base.race_std_idm,
     base.race_std_ten,
     base.race_std_agari,
     base.tyb_total_score,
     base.tyb_track_cond,
     base.jvd_cushion_val,
     base.sire_system_code,
     base.bms_system_code,
     base.gekiso_index,
     base.dist_aptitude,
     base.stable_eval_code,
     base.training_arrow_code,
     base.gekiso_dist_cross,
     base.training_stable_cross,
     base.grass_aptitude_code,
     base.dirt_aptitude_code,
     base.running_style_code,
     base.mud_aptitude_code,
     base.running_form_code,
     base.body_type_code,
     base.rotation_days,
     base.dist_aptitude2,
     base.longshot_index,
     base.start_index,
     base.late_start_rate,
     base.popular_index,
     base.jockey_exp_rentai,
     base.jockey_exp_win,
     base.jockey_exp_top3,
     base.track_fit_signal,
     base.tt_bias_score,
     base.tt_alpha_signal,
     base.tt_confidence,
     base.tt_video_count,
     base.low_pop_high_idm,
     base.pace_advantage_score,
     base.actual_rank,
     base.is_winner,
     base.is_top3,
     base.rank_ratio,
     base.rotation_seg,
     base.gate_style_cross,
     base.gate_bias_score,
     base.dist_category,
     base.gate_style_dist_cross,
     base.style_rotation_cross,
     base.short_rest_high_idm,
     base.tt_idm_cross,
     base.tyb_paddock_score,
     base.tyb_horse_weight,
     base.tyb_weight_change,
     base.tyb_vibe_code,
     base.tyb_jockey_score,
     base.tyb_info_score,
     base.tyb_odds_score,
     base.tyb_cho_total_mark,
     base.tyb_idm,
     base.tyb_win_odds,
     base.tyb_place_odds,
     base.tyb_equipment_change,
     base.tyb_leg_info,
     base.tyb_body_code,
     base.h1_bias_score,
     base.h1_alpha_signal,
     base.h1_confidence,
     base.h1_video_count,
     base.feat_vg_fu2,
     base.feat_vg_yusen,
     base.feat_vg_f_index,
     base.h1_idm_cross,
     base.moe_regime,
     base.feat_causal_jockey_change,
     base.feat_causal_short_interval,
     base.feat_deriv_idm_delta,
     base.feat_deriv_ten_delta,
     base.feat_deriv_agari_delta,
     base.feat_deriv_improving,
     base.feat_blood_sire_cond_top3,
     base.feat_blood_sire_cond_sample,
     base.feat_blood_sire_hot_cond,
     base.feat_sf_speed_figure,
     base.feat_tb_inner_bias,
     -- Phase 421: RESOLVED — inline golden rule (replaces mv_heritage_golden_rules dependency)
     CASE
         WHEN COALESCE(base.jockey_exp_top3, 0) >= 0.40
           OR COALESCE(base.tyb_total_score, 0) >= 60
           OR COALESCE(base.idm_zscore, 0) >= 1.5
         THEN 1
         ELSE 0
     END AS feat_heritage_golden_match,
     -- RESOLVED: Topological chaos from pace stats WINDOW function
     COALESCE(base.w_topological_chaos, 0) AS feat_topological_chaos,
     -- RESOLVED: MAD pace resistance from WINDOW function
     COALESCE(base.w_mad_pace_resistance, 0) AS feat_mad_pace_resistance,
     -- RESOLVED: MAD (Median Absolute Deviation) of core indices
     COALESCE(base.w_score_idm_mad, 0) AS feat_score_idm_mad,
     COALESCE(base.w_idx_ten_mad, 0) AS feat_idx_ten_mad,
     COALESCE(base.w_idx_agari_mad, 0) AS feat_idx_agari_mad,
     -- RESOLVED: Race-level standard deviation (also available as race_std_*)
     COALESCE(base.w_race_std_score_idm, 0) AS feat_race_std_score_idm,
     COALESCE(base.w_race_std_idx_ten, 0) AS feat_race_std_idx_ten,
     COALESCE(base.w_race_std_idx_agari, 0) AS feat_race_std_idx_agari,
     -- RESOLVED: Race-level mean of core indices
     COALESCE(base.w_race_mean_score_idm, 0) AS feat_race_mean_score_idm,
     COALESCE(base.w_race_mean_idx_ten, 0) AS feat_race_mean_idx_ten,
     COALESCE(base.w_race_mean_idx_agari, 0) AS feat_race_mean_idx_agari,
     COALESCE(base.class_code_num, 0) AS feat_race_class_cat,
     COALESCE(base.sire_system_code, (0)::numeric) AS feat_sire_cat,
     COALESCE(base.bms_system_code, (0)::numeric) AS feat_bms_cat,
     -- is_chaos: NOW uses computed topological_chaos and mad_pace_resistance
     -- instead of hardcoded 0.0 values
         CASE
             WHEN ((((
             CASE
                 WHEN (COALESCE(base.race_std_idm, 10.0) < 4.0) THEN 1
                 ELSE 0
             END +
             CASE
                 WHEN (COALESCE(base.race_std_ten, 10.0) < 5.0) THEN 1
                 ELSE 0
             END) +
              CASE
                  -- Phase 405 fix: STDDEV_POP >= 0 always, so < -10 was impossible
                  -- 0.65 ≈ P10 of distribution (low-variance = uniform race)
                  WHEN (COALESCE(base.w_topological_chaos, 0) < 0.65) THEN 1
                  ELSE 0
              END) +
              CASE
                  -- Phase 405 fix: MAX=2.62, so > 20 was impossible
                  -- 1.5 ≈ P93 (high deviation from race mean pace)
                  WHEN (COALESCE(base.w_mad_pace_resistance, 0) > 1.5) THEN 1
                  ELSE 0
              END) >= 3) THEN true
             ELSE false
         END AS is_chaos,
         CASE
             WHEN ((COALESCE(base.surface_type, 'Turf'::text) = 'Steeple'::text) OR (COALESCE(base.is_hurdle, 0) = 1)) THEN 'Hurdle'::text
             WHEN (COALESCE(base.distance_m, 9999) <= 1300) THEN
             CASE
                 WHEN (COALESCE(base.surface_type, 'Turf'::text) = 'Turf'::text) THEN 'Sprint_Turf'::text
                 ELSE 'Sprint_Dirt'::text
             END
             WHEN ((((
             CASE
                 WHEN (COALESCE(base.race_std_idm, 10.0) < 4.0) THEN 1
                 ELSE 0
             END +
             CASE
                 WHEN (COALESCE(base.race_std_ten, 10.0) < 5.0) THEN 1
                 ELSE 0
             END) +
              CASE
                  WHEN (COALESCE(base.w_topological_chaos, 0) < 0.65) THEN 1
                  ELSE 0
              END) +
              CASE
                  WHEN (COALESCE(base.w_mad_pace_resistance, 0) > 1.5) THEN 1
                  ELSE 0
              END) >= 3) THEN
             CASE
                 WHEN (COALESCE(base.surface_type, 'Turf'::text) = 'Turf'::text) THEN 'Chaos_Turf'::text
                 ELSE 'Chaos_Dirt'::text
             END
             ELSE
             CASE
                 WHEN (COALESCE(base.surface_type, 'Turf'::text) = 'Turf'::text) THEN 'Core_Turf'::text
                 ELSE 'Core_Dirt'::text
             END
         END AS regime
FROM base
