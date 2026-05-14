{{ config(
    materialized='view',
    schema='api'
) }}

WITH base AS (
    SELECT
        l.target_date, l.venue_code, l.race_no, l.umaban, l.regime,
        k.枠番 as wakuban,
        k.distance,
        k.class_code,
        CAST(NULLIF(k.score_idm, '') AS numeric) as score_idm,
        CAST(NULLIF(k.idx_ten, '') AS numeric) as idx_ten,
        CAST(NULLIF(k.idx_agari, '') AS numeric) as idx_agari,
        CAST(NULLIF(k.idx_pace, '') AS numeric) as idx_pace,
        CAST(NULLIF(k.info_score, '') AS numeric) as info_score,
        CAST(NULLIF(k.total_score, '') AS numeric) as total_score,
        CASE WHEN TRIM(s.着順) ~ '^[0-9]+$' THEN CAST(TRIM(s.着順) AS INTEGER) ELSE NULL END as finish_pos,
        CAST(REPLACE(COALESCE(NULLIF(TRIM(s.単勝), ''), '0'), ',', '') AS numeric) as tansho_return,
        CAST(REPLACE(COALESCE(NULLIF(TRIM(s.複勝), ''), '0'), ',', '') AS numeric) as fukusho_return
    FROM {{ source('api', 'a3_inference_logs') }} l
    JOIN {{ source('public', 'kyi_parsed') }} k
      ON l.target_date = k.race_date
     AND l.venue_code  = k.venue_code
     AND CAST(l.race_no AS INTEGER) = CAST(k.race_no AS INTEGER)
     AND CAST(l.umaban  AS INTEGER) = CAST(k.umaban  AS INTEGER)
    JOIN {{ source('public', 'sed_parsed') }} s
      ON l.target_date = s.race_date
     AND l.venue_code  = s.場コード
     AND CAST(l.race_no AS INTEGER) = CAST(s.Ｒ      AS INTEGER)
     AND CAST(l.umaban  AS INTEGER) = CAST(s.馬番    AS INTEGER)
    WHERE l.meta_score > 0
      AND l.target_date >= '20240101'
)
SELECT
    target_date, venue_code, race_no, umaban, regime, wakuban, distance, class_code,
    finish_pos, tansho_return, fukusho_return,
    CASE WHEN finish_pos = 1 THEN 1 ELSE 0 END as is_win,
    CASE WHEN finish_pos <= 2 THEN 1 ELSE 0 END as is_top2,
    CASE WHEN finish_pos <= 3 THEN 1 ELSE 0 END as is_top3,
    CASE WHEN finish_pos = 1 THEN tansho_return ELSE 0 END as target_tansho_roi,
    CASE WHEN finish_pos <= 3 THEN fukusho_return ELSE 0 END as target_fukusho_roi,
    CASE WHEN finish_pos <= 2 THEN 1 ELSE 0 END as target_top2,
    CASE WHEN finish_pos <= 3 THEN 1 ELSE 0 END as target_top3,
    
    COUNT(umaban) OVER (PARTITION BY target_date, venue_code, race_no) as field_size,
    
    AVG(score_idm) OVER (PARTITION BY target_date, venue_code, race_no) as idm_mean,
    COALESCE(STDDEV(score_idm) OVER (PARTITION BY target_date, venue_code, race_no), 0) as idm_std,
    score_idm - AVG(score_idm) OVER (PARTITION BY target_date, venue_code, race_no) as idm_diff,
    
    AVG(idx_ten) OVER (PARTITION BY target_date, venue_code, race_no) as ten_mean,
    COALESCE(STDDEV(idx_ten) OVER (PARTITION BY target_date, venue_code, race_no), 0) as ten_std,
    idx_ten - AVG(idx_ten) OVER (PARTITION BY target_date, venue_code, race_no) as ten_diff,
    
    AVG(idx_agari) OVER (PARTITION BY target_date, venue_code, race_no) as agari_mean,
    COALESCE(STDDEV(idx_agari) OVER (PARTITION BY target_date, venue_code, race_no), 0) as agari_std,
    idx_agari - AVG(idx_agari) OVER (PARTITION BY target_date, venue_code, race_no) as agari_diff,
    
    AVG(idx_pace) OVER (PARTITION BY target_date, venue_code, race_no) as pace_mean,
    COALESCE(STDDEV(idx_pace) OVER (PARTITION BY target_date, venue_code, race_no), 0) as pace_std,
    idx_pace - AVG(idx_pace) OVER (PARTITION BY target_date, venue_code, race_no) as pace_diff,
    
    AVG(info_score) OVER (PARTITION BY target_date, venue_code, race_no) as info_mean,
    COALESCE(STDDEV(info_score) OVER (PARTITION BY target_date, venue_code, race_no), 0) as info_std,
    info_score - AVG(info_score) OVER (PARTITION BY target_date, venue_code, race_no) as info_diff,
    
    AVG(total_score) OVER (PARTITION BY target_date, venue_code, race_no) as total_mean,
    COALESCE(STDDEV(total_score) OVER (PARTITION BY target_date, venue_code, race_no), 0) as total_std,
    total_score - AVG(total_score) OVER (PARTITION BY target_date, venue_code, race_no) as total_diff
FROM base
WHERE finish_pos IS NOT NULL
