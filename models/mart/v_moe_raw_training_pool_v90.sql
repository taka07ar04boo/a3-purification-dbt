{{ config(
    materialized='view',
    schema='api',
    enabled=false
) }}

SELECT
    sf.race_date,
    sf.venue_code,
    sf.race_no,
    sf.umaban,
    sf.horse_id,
    sf.horse_name,
    sf.jockey_code,
    sf.trainer_code,
    sf.is_hurdle,
    sf.distance_m,
    sf.total_horses,

    sf.idm_zscore,
    sf.ten_zscore,
    sf.agari_zscore,
    sf.race_std_idm,
    sf.race_std_ten,
    sf.race_std_agari,
    
    sf.odds_win_pre,
    sf.pop_win_pre,
    sf.low_pop_high_idm,
    sf.pace_advantage_score,
    sf.track_fit_signal,
    
    sf.jvd_cushion_val,
    sf.rotation_days,
    sf.jockey_exp_win,
    sf.jockey_exp_top3,
    
    sf.tt_bias_score,
    sf.tt_alpha_signal,
    sf.tt_confidence,

    sf.start_index,
    sf.popular_index,
    sf.longshot_index,
    sf.gekiso_index,

    sf.surface_type,

    t.kakutei_chakujun::integer AS actual_rank,
    CASE WHEN t.kakutei_chakujun::integer = 1 THEN 1 ELSE 0 END AS is_winner,
    CASE WHEN t.kakutei_chakujun::integer <= 3 THEN 1 ELSE 0 END AS is_top3,
    CASE WHEN t.kakutei_chakujun::integer <= 5 THEN 1 ELSE 0 END AS is_top5

FROM api.mv_unified_features_v52 sf
JOIN public.jvd_se t
    ON sf.race_date = (t.kaisai_nen || t.kaisai_tsukihi)
    AND sf.venue_code = t.keibajo_code
    AND sf.race_no = t.race_bango
    AND sf.umaban = lpad(t.umaban::text, 2, '0')
WHERE
    t.kakutei_chakujun IS NOT NULL 
    AND t.kakutei_chakujun ~ '^[0-9]+$'
    AND t.kakutei_chakujun::integer > 0
    AND (t.ijo_kubun_code IS NULL OR t.ijo_kubun_code = '0')
