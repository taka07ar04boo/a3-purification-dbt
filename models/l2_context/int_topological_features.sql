{{ config(materialized='view') }}

WITH source_data AS (
    SELECT 
        k.race_key,
        CASE WHEN TRIM(k.score_idm) ~ '^-?[0-9.]+$' THEN k.score_idm::numeric ELSE 0 END AS feat_score_idm,
        CASE WHEN TRIM(k.idx_ten) ~ '^-?[0-9.]+$' THEN k.idx_ten::numeric ELSE 0 END AS feat_idx_ten,
        CASE WHEN TRIM(k.idx_agari) ~ '^-?[0-9.]+$' THEN k.idx_agari::numeric ELSE 0 END AS feat_idx_agari,
        CASE WHEN TRIM(k."ペース指数") ~ '^-?[0-9.]+$' THEN k."ペース指数"::numeric ELSE 0 END AS feat_pace_advantage_score,
        CASE WHEN TRIM(k.odds_win) ~ '^-?[0-9.]+$' THEN k.odds_win::numeric ELSE 0 END AS feat_odds_win_pre,
        CASE WHEN TRIM(t."情報指数") ~ '^[-+]?[0-9]*\.?[0-9]+$' THEN TRIM(t."情報指数")::numeric ELSE 0 END AS tyb_info_score
    FROM public.kyi_parsed k
    LEFT JOIN public.tyb_parsed t ON t.race_date = k.race_date AND t.race_key = k.race_key AND t.umaban::integer = k.umaban::integer
    WHERE k.race_date = '{{ var("target_date", "") }}'
),
race_stats AS (
    SELECT 
        race_key,
        STDDEV_SAMP(feat_score_idm) AS feat_env_std_raw_score_idm,
        AVG(feat_score_idm) AS feat_env_mean_raw_score_idm,
        STDDEV_SAMP(feat_idx_ten) AS feat_env_std_raw_idx_ten,
        AVG(feat_idx_ten) AS feat_env_mean_raw_idx_ten,
        STDDEV_SAMP(feat_idx_agari) AS feat_env_std_raw_idx_agari,
        AVG(feat_idx_agari) AS feat_env_mean_raw_idx_agari,
        STDDEV_SAMP(feat_pace_advantage_score) AS feat_env_std_idx_pace,
        AVG(feat_pace_advantage_score) AS feat_env_mean_idx_pace,
        STDDEV_SAMP(tyb_info_score) AS feat_env_std_tyb_info,
        AVG(tyb_info_score) AS feat_env_mean_tyb_info,
        -- Calculate odds entropy (proxy for chaos)
        COALESCE(-SUM((1.0 / NULLIF(feat_odds_win_pre, 0)) * LN(NULLIF((1.0 / NULLIF(feat_odds_win_pre, 0)), 0))), 0) AS feat_chaos_odds_entropy,
        MAX(feat_score_idm) - MIN(feat_score_idm) AS feat_chaos_idm_spread,
        COUNT(*) AS feat_field_size
    FROM source_data
    GROUP BY race_key
)
SELECT * FROM race_stats
