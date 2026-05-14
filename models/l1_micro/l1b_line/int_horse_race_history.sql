-- int_horse_race_history

-- dbt/models/l1_micro/l1b_line/int_horse_race_history.sql

WITH base AS (
    SELECT
        se.ketto_toroku_bango as horse_id,
        TO_DATE(se.kaisai_nen || se.kaisai_tsukihi, 'YYYYMMDD') as race_date,
        se.keibajo_code as venue,
        se.race_bango::int as race,
        CAST(NULLIF(se.kakutei_chakujun, '') AS integer) as rank,
        CAST(NULLIF(se.tansho_ninkijun, '') AS integer) as popularity,
        CAST(NULLIF(se.tansho_odds, '') AS numeric)/10.0 as win_odds,
        CAST(REPLACE(NULLIF(se.time_sa, ''), '+', '') AS numeric)/10.0 as time_diff,
        CAST(NULLIF(se.kohan_3f, '') AS numeric)/10.0 as last_3f,
        CAST(NULLIF(se.corner_4, '') AS integer) as c4_pos,
        CAST(NULLIF(se.futan_juryo, '') AS numeric)/10.0 as weight,
        CAST(NULLIF(ra.kyori, '') AS integer) as distance
    FROM {{ source('public', 'jvd_se') }} se
    JOIN {{ source('public', 'jvd_ra') }} ra
      ON se.kaisai_nen = ra.kaisai_nen 
     AND se.kaisai_tsukihi = ra.kaisai_tsukihi 
     AND se.keibajo_code = ra.keibajo_code 
     AND se.race_bango = ra.race_bango
    WHERE se.kakutei_chakujun ~ '^[0-9]+$' AND NULLIF(se.tansho_odds, '') IS NOT NULL
),
race_stats AS (
    SELECT
        *,
        -- レース内での上がり3F順位
        RANK() OVER (PARTITION BY race_date, venue, race ORDER BY last_3f ASC) as last_3f_rank,
        -- レース内の出走頭数
        COUNT(*) OVER (PARTITION BY race_date, venue, race) as field_size
    FROM base
),
flags AS (
    SELECT
        horse_id,
        race_date,
        -- === [★真の好走（Real Over-Performance）フラグ] ===
        -- 1. 馬券内 (標準的な好走)
        CASE WHEN rank <= 3 THEN 1 ELSE 0 END as is_place,
        -- 2. タイム差激小 (着順に関わらず0.3秒差以内)
        CASE WHEN time_diff <= 0.3 THEN 1 ELSE 0 END as is_close_time,
        -- 3. 大穴掲示板 (10番人気以下での掲示板)
        CASE WHEN rank <= 5 AND popularity >= 10 THEN 1 ELSE 0 END as is_hidden_board,
        -- 4. 上がり最速の敗者 (上がり1位で4着以下)
        CASE WHEN last_3f_rank = 1 AND rank >= 4 THEN 1 ELSE 0 END as is_fastest_loser,
        
        -- === [バフ・デバフ用フラグ] ===
        CASE WHEN rank = 1 THEN 1 ELSE 0 END as is_win,
        c4_pos,
        popularity,
        rank,
        weight,
        win_odds,
        distance
    FROM race_stats
),
seq AS (
    SELECT
        *,
        -- 真の好走フラグ（どれか1つでも満たせばエンジン優秀）
        CASE 
            WHEN is_place=1 OR is_close_time=1 OR is_hidden_board=1 OR is_fastest_loser=1 THEN 1.0 
            ELSE 0.0 
        END as is_real_koso,
        -- 全履歴の集計用
        RANK() OVER (PARTITION BY horse_id ORDER BY race_date DESC) as run_history_id
    FROM flags
)
SELECT
    horse_id,
    COUNT(*) as total_starts,
    -- 基礎回収率
    COALESCE(SUM(CASE WHEN is_win=1 THEN win_odds * 100 ELSE 0 END) / NULLIF(COUNT(*), 0), 0) as win_roi,
    AVG(is_win) as win_rate,
    AVG(is_place) as place_rate,
    
    -- 真の好走率（全レースにおけるエンジン発揮率）
    AVG(is_real_koso) as real_koso_rate,
    
    -- 直近レースから抽出する「突然変異判定用」特徴量 (L5へパスする)
    MAX(CASE WHEN run_history_id = 1 THEN rank END) as prev_rank,
    MAX(CASE WHEN run_history_id = 1 THEN popularity END) as prev_pop,
    MAX(CASE WHEN run_history_id = 1 THEN c4_pos END) as prev_c4_pos,
    MAX(CASE WHEN run_history_id = 1 THEN weight END) as prev_weight,
    MAX(CASE WHEN run_history_id = 1 THEN distance END) as prev_distance,
    MAX(CASE WHEN run_history_id = 1 THEN race_date END) as prev_race_date

FROM seq
GROUP BY horse_id
