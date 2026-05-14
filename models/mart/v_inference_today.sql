{{ config(
    materialized='view',
    schema='api'
) }}

-- v_inference_today: 推論チェーン最上流ビュー (Phase 410 dbt化)
-- 元は api.v_inference_today (18,315文字) のレガシー手動ビュー
-- kyi_parsed_v51 をベースに、bac/tyb/ukc/jvd/taketube/vg を結合して
-- 全推論用特徴量ベクトルを構築する

WITH race_conditions AS (
    SELECT DISTINCT ON (bac.race_date, bac."場コード", lpad(bac."回", 2, '0'))
        bac.race_date,
        bac."場コード" AS venue_code,
        lpad(bac."回", 2, '0') AS race_no,
        CASE
            WHEN bac."距離" ~ '^[0-9]+$' THEN bac."距離"::integer
            ELSE NULL::integer
        END AS distance_m,
        bac."芝ダ障害コード" AS track_code,
        CASE bac."芝ダ障害コード"
            WHEN '1' THEN 'Turf'
            WHEN '2' THEN 'Dirt'
            WHEN '3' THEN 'Steeple'
            ELSE NULL
        END AS surface_type
    FROM {{ source('public', 'bac_parsed') }} bac
    WHERE bac."距離" IS NOT NULL AND bac."距離" <> ''
),

kyi_base AS (
    SELECT
        k.race_date,
        k.race_key,
        substring(k.race_key, 1, 2) AS venue_code,
        lpad(substring(k.race_key, 7, 2), 2, '0') AS race_no_str,
        lpad(k.umaban, 2, '0') AS umaban,
        k.horse_id,
        k.horse_name,
        k.jockey_code,
        k.trainer_code,
        CASE WHEN k."枠番" ~ '^[0-9]$' THEN k."枠番"::integer ELSE NULL::integer END AS gate_no,
        CASE WHEN k.score_idm ~ '^[0-9.]+$' THEN k.score_idm::numeric ELSE 0::numeric END AS kyi_idm,
        CASE WHEN k.idx_ten ~ '^[0-9.]+$' THEN k.idx_ten::numeric ELSE 0::numeric END AS kyi_ten,
        CASE WHEN k.idx_agari ~ '^[0-9.]+$' THEN k.idx_agari::numeric ELSE 0::numeric END AS kyi_agari,
        CASE WHEN k.odds_win ~ '^[0-9.]+$' THEN k.odds_win::numeric ELSE NULL::numeric END AS kyi_odds,
        CASE WHEN k.pop_win ~ '^[0-9]+$' THEN k.pop_win::integer ELSE NULL::integer END AS kyi_pop,
        CASE WHEN k.class_code ~ '^[0-9]{1,4}$' THEN k.class_code::integer ELSE 0 END AS class_num,
        CASE WHEN k."激走指数" ~ '^[0-9.]+$' THEN k."激走指数"::numeric ELSE 0::numeric END AS gekiso_index,
        CASE WHEN k."距離適性" ~ '^[0-9.]+$' THEN k."距離適性"::numeric ELSE 0::numeric END AS dist_aptitude,
        CASE WHEN k."厩舎評価コード" ~ '^[0-9.]+$' THEN k."厩舎評価コード"::numeric ELSE 0::numeric END AS stable_eval_code,
        CASE WHEN k."調教矢印コード" ~ '^[0-9.]+$' THEN k."調教矢印コード"::numeric ELSE 0::numeric END AS training_arrow_code,
        CASE WHEN k."芝適性コード" ~ '^[0-9.]+$' THEN k."芝適性コード"::numeric ELSE 0::numeric END AS grass_aptitude_code,
        CASE WHEN k."ダ適性コード" ~ '^[0-9.]+$' THEN k."ダ適性コード"::numeric ELSE 0::numeric END AS dirt_aptitude_code,
        CASE WHEN k."脚質" ~ '^[0-9.]+$' THEN k."脚質"::numeric ELSE 0::numeric END AS running_style_code,
        CASE WHEN k."重適正コード" ~ '^[0-9.]+$' THEN k."重適正コード"::numeric ELSE 0::numeric END AS mud_aptitude_code,
        CASE WHEN k."走法" ~ '^[0-9.]+$' THEN k."走法"::numeric ELSE 0::numeric END AS running_form_code,
        CASE WHEN k."体型" ~ '^[0-9.]+$' THEN k."体型"::numeric ELSE 0::numeric END AS body_type_code,
        CASE WHEN k."ローテーション" ~ '^[0-9.]+$' THEN k."ローテーション"::numeric ELSE 0::numeric END AS rotation_days,
        CASE WHEN k."距離適性２" ~ '^[0-9.]+$' THEN k."距離適性２"::numeric ELSE 0::numeric END AS dist_aptitude2,
        CASE WHEN k."万券指数" ~ '^[0-9.]+$' THEN k."万券指数"::numeric ELSE 0::numeric END AS longshot_index,
        CASE WHEN k."馬スタート指数" ~ '^[0-9.]+$' THEN k."馬スタート指数"::numeric ELSE 0::numeric END AS start_index,
        CASE WHEN k."馬出遅率" ~ '^[0-9.]+$' THEN k."馬出遅率"::numeric ELSE 0::numeric END AS late_start_rate,
        CASE WHEN k."人気指数" ~ '^[0-9.]+$' THEN k."人気指数"::numeric ELSE 0::numeric END AS popular_index,
        CASE WHEN k."騎手期待連対率" ~ '^[0-9.]+$' THEN k."騎手期待連対率"::numeric ELSE 0::numeric END AS jockey_exp_rentai,
        CASE WHEN k."騎手期待単勝率" ~ '^[0-9.]+$' THEN k."騎手期待単勝率"::numeric ELSE 0::numeric END AS jockey_exp_win,
        CASE WHEN k."騎手期待３着内率" ~ '^[0-9.]+$' THEN k."騎手期待３着内率"::numeric ELSE 0::numeric END AS jockey_exp_top3
    FROM {{ source('public', 'kyi_parsed_v51') }} k
),

race_cond_dedup AS (
    SELECT DISTINCT ON (race_date, venue_code, race_no)
        race_date,
        venue_code,
        race_no,
        distance_m,
        track_code,
        surface_type
    FROM race_conditions
    ORDER BY race_date, venue_code, race_no
),

race_stats AS (
    SELECT
        k.race_date,
        k.venue_code,
        k.race_no_str AS race_no,
        avg(k.kyi_idm) AS avg_idm,
        avg(k.kyi_ten) AS avg_ten,
        avg(k.kyi_agari) AS avg_agari,
        stddev_pop(k.kyi_idm) AS std_idm,
        stddev_pop(k.kyi_ten) AS std_ten,
        stddev_pop(k.kyi_agari) AS std_agari,
        count(*) AS total_horses
    FROM kyi_base k
    GROUP BY k.race_date, k.venue_code, k.race_no_str
),

tyb_agg AS (
    SELECT DISTINCT ON (tyb.race_date, tyb.race_key, tyb.umaban)
        tyb.race_date,
        tyb.race_key,
        tyb.umaban,
        CASE WHEN TRIM(tyb."総合指数") ~ '^[0-9.]+$' THEN TRIM(tyb."総合指数")::numeric ELSE 0::numeric END AS tyb_total_score,
        CASE WHEN TRIM(tyb."馬場状態コード") ~ '^[0-9.]+$' THEN TRIM(tyb."馬場状態コード")::numeric ELSE 0::numeric END AS tyb_track_cond,
        CASE WHEN TRIM(tyb."パドック指数") ~ '^[-+]?[0-9]*\.?[0-9]+$' THEN TRIM(tyb."パドック指数")::numeric ELSE 0::numeric END AS tyb_paddock_score,
        CASE WHEN TRIM(tyb."馬体重") ~ '^[-+]?[0-9]*\.?[0-9]+$' THEN TRIM(tyb."馬体重")::numeric ELSE 0::numeric END AS tyb_horse_weight,
        CASE WHEN TRIM(tyb."馬体重増減") ~ '^[-+]?[0-9]*\.?[0-9]+$' THEN TRIM(tyb."馬体重増減")::numeric ELSE 0::numeric END AS tyb_weight_change,
        CASE WHEN TRIM(tyb."気配コード") ~ '^[-+]?[0-9]*\.?[0-9]+$' THEN TRIM(tyb."気配コード")::numeric ELSE 0::numeric END AS tyb_vibe_code,
        CASE WHEN TRIM(tyb."騎手指数") ~ '^[-+]?[0-9]*\.?[0-9]+$' THEN TRIM(tyb."騎手指数")::numeric ELSE 0::numeric END AS tyb_jockey_score,
        CASE WHEN TRIM(tyb."情報指数") ~ '^[-+]?[0-9]*\.?[0-9]+$' THEN TRIM(tyb."情報指数")::numeric ELSE 0::numeric END AS tyb_info_score,
        CASE WHEN TRIM(tyb."オッズ指数") ~ '^[-+]?[0-9]*\.?[0-9]+$' THEN TRIM(tyb."オッズ指数")::numeric ELSE 0::numeric END AS tyb_odds_score,
        CASE WHEN TRIM(tyb."単勝オッズ") ~ '^[-+]?[0-9]*\.?[0-9]+$' THEN TRIM(tyb."単勝オッズ")::numeric ELSE 0::numeric END AS tyb_win_odds,
        CASE WHEN TRIM(tyb."複勝オッズ") ~ '^[-+]?[0-9]*\.?[0-9]+$' THEN TRIM(tyb."複勝オッズ")::numeric ELSE 0::numeric END AS tyb_place_odds,
        CASE WHEN TRIM(tyb."直前総合印") ~ '^[-+]?[0-9]*\.?[0-9]+$' THEN TRIM(tyb."直前総合印")::numeric ELSE 0::numeric END AS tyb_final_mark
    FROM {{ source('public', 'tyb_parsed') }} tyb
    ORDER BY tyb.race_date, tyb.race_key, tyb.umaban, tyb.id DESC
),

ukc_agg AS (
    SELECT
        ukc.race_date,
        ukc.race_key,
        CASE WHEN TRIM(ukc."父系統コード") ~ '^[0-9]+$' THEN TRIM(ukc."父系統コード")::numeric ELSE 0::numeric END AS sire_system_code,
        CASE WHEN TRIM(ukc."母父系統コード") ~ '^[0-9]+$' THEN TRIM(ukc."母父系統コード")::numeric ELSE 0::numeric END AS bms_system_code
    FROM {{ source('public', 'ukc_parsed_v50') }} ukc
),

jvd_agg AS (
    -- Phase 410: クッション値は a3_jra_cushion 経由で取得
    -- 元ビューは api.v_jravan_ra (WHERE false スタブ) を参照していたため実質無効だった
    -- a3_jra_cushion はレース番号なし(場単位)なのでvenue_code単位でJOIN
    SELECT
        c.target_date AS race_date,
        c.venue AS venue_code,
        c.cushion_val
    FROM {{ source('public', 'a3_jra_cushion') }} c
    WHERE c.cushion_val IS NOT NULL
),

taketube_agg AS (
    SELECT
        to_char(tt.race_date::timestamp, 'YYYYMMDD') AS race_date,
        tt.venue_code,
        lpad(tt.race_no::text, 2, '0') AS race_no,
        COALESCE(avg(tt.bias_score), 0) AS tt_bias_score,
        COALESCE(avg(tt.alpha_signal), 0) AS tt_alpha_signal,
        COALESCE(avg(tt.confidence), 0.5) AS tt_confidence,
        count(*) AS tt_video_count
    FROM {{ source('api', 'a3_taketube_insights') }} tt
    WHERE tt.race_date IS NOT NULL
    GROUP BY to_char(tt.race_date::timestamp, 'YYYYMMDD'), tt.venue_code, tt.race_no
),

vg_data AS (
    SELECT
        ne."年月日" AS vg_date,
        ne."Ｒ" AS race_no,
        ne."馬番" AS umaban,
        COALESCE(ne."Ｆ指数", 0)::numeric AS vg_f_index,
        0::numeric AS vg_fu2,
        COALESCE(ne."優先値", 0)::numeric AS vg_yusen
    FROM {{ source('public', 'ne') }} ne
)

SELECT
    k.race_date,
    k.venue_code,
    k.race_no_str AS race_no,
    k.umaban,
    k.horse_id,
    k.horse_name,
    k.jockey_code,
    k.trainer_code,
    k.gate_no,
    k.kyi_idm AS score_idm,
    k.kyi_ten AS idx_ten,
    k.kyi_agari AS idx_agari,
    k.kyi_odds AS odds_win_pre,
    k.kyi_pop AS pop_win_pre,
    COALESCE(rc.distance_m, 1600) AS distance_m,
    r.total_horses::integer AS total_horses,
    k.class_num AS class_code_num,
    CASE
        WHEN COALESCE(rc.surface_type, 'Turf') = 'Steeple' THEN 1
        ELSE 0
    END AS is_hurdle,
    COALESCE(rc.surface_type, 'Turf') AS surface_type,
    CASE
        WHEN r.std_idm > 0 THEN (k.kyi_idm - r.avg_idm) / r.std_idm
        ELSE 0::numeric
    END AS idm_zscore,
    CASE
        WHEN r.std_ten > 0 THEN (k.kyi_ten - r.avg_ten) / r.std_ten
        ELSE 0::numeric
    END AS ten_zscore,
    CASE
        WHEN r.std_agari > 0 THEN (k.kyi_agari - r.avg_agari) / r.std_agari
        ELSE 0::numeric
    END AS agari_zscore,
    r.std_idm AS race_std_idm,
    r.std_ten AS race_std_ten,
    r.std_agari AS race_std_agari,
    COALESCE(t.tyb_total_score, 0::numeric) AS tyb_total_score,
    COALESCE(t.tyb_track_cond, 0::numeric) AS tyb_track_cond,
    COALESCE(j.cushion_val, 9.5) AS jvd_cushion_val,
    COALESCE(uc.sire_system_code, 0::numeric) AS sire_system_code,
    COALESCE(uc.bms_system_code, 0::numeric) AS bms_system_code,
    k.gekiso_index,
    k.dist_aptitude,
    k.stable_eval_code,
    k.training_arrow_code,
    k.gekiso_index * GREATEST(k.dist_aptitude, 1::numeric) AS gekiso_dist_cross,
    k.training_arrow_code * k.stable_eval_code AS training_stable_cross,
    k.grass_aptitude_code,
    k.dirt_aptitude_code,
    k.running_style_code,
    k.mud_aptitude_code,
    k.running_form_code,
    k.body_type_code,
    k.rotation_days,
    k.dist_aptitude2,
    k.longshot_index,
    k.start_index,
    k.late_start_rate,
    k.popular_index,
    k.jockey_exp_rentai,
    k.jockey_exp_win,
    k.jockey_exp_top3,
    CASE
        WHEN COALESCE(rc.surface_type, 'Turf') <> 'Steeple' AND k.grass_aptitude_code = 1 THEN 2
        WHEN COALESCE(rc.surface_type, 'Turf') <> 'Steeple' AND k.grass_aptitude_code = 2 THEN 1
        WHEN COALESCE(rc.surface_type, 'Turf') <> 'Steeple' AND k.grass_aptitude_code = 3 THEN -1
        ELSE 0
    END AS track_fit_signal,
    COALESCE(tt.tt_bias_score, 0::double precision) AS tt_bias_score,
    COALESCE(tt.tt_alpha_signal, 0::double precision) AS tt_alpha_signal,
    COALESCE(tt.tt_confidence, 0::double precision) AS tt_confidence,
    COALESCE(tt.tt_video_count, 0::bigint) AS tt_video_count,
    CASE
        WHEN k.kyi_pop >= 6 AND
            CASE WHEN r.std_idm > 0 THEN (k.kyi_idm - r.avg_idm) / r.std_idm ELSE 0::numeric END > 0.5
        THEN 1
        ELSE 0
    END AS low_pop_high_idm,
    CASE k.running_style_code
        WHEN 1 THEN 2
        WHEN 2 THEN 1
        WHEN 3 THEN 0
        WHEN 4 THEN -1
        ELSE 0
    END AS pace_advantage_score,
    NULL::integer AS actual_rank,
    0 AS is_winner,
    0 AS is_top3,
    0.5::double precision AS rank_ratio,
    COALESCE(t.tyb_paddock_score, 0::numeric) AS tyb_paddock_score,
    COALESCE(t.tyb_horse_weight, 0::numeric) AS tyb_horse_weight,
    COALESCE(t.tyb_weight_change, 0::numeric) AS tyb_weight_change,
    COALESCE(t.tyb_vibe_code, 0::numeric) AS tyb_vibe_code,
    COALESCE(t.tyb_jockey_score, 0::numeric) AS tyb_jockey_score,
    COALESCE(t.tyb_info_score, 0::numeric) AS tyb_info_score,
    COALESCE(t.tyb_odds_score, 0::numeric) AS tyb_odds_score,
    COALESCE(t.tyb_win_odds, 0::numeric) AS tyb_win_odds,
    COALESCE(t.tyb_place_odds, 0::numeric) AS tyb_place_odds,
    COALESCE(t.tyb_final_mark, 0::numeric) AS tyb_final_mark,
    COALESCE(vg.vg_f_index, 0::numeric) AS vg_f_index,
    COALESCE(vg.vg_fu2, 0::numeric) AS vg_fu2,
    COALESCE(vg.vg_yusen, 0::numeric) AS vg_yusen,
    CASE
        WHEN COALESCE(rc.surface_type, 'Turf') = 'Steeple' THEN 'Hurdle'
        WHEN COALESCE(rc.distance_m, 9999) <= 1300 THEN
            CASE WHEN COALESCE(rc.surface_type, 'Turf') = 'Turf' THEN 'Sprint_Turf' ELSE 'Sprint_Dirt' END
        WHEN (
            CASE WHEN COALESCE(r.std_idm, 10) < 4 THEN 1 ELSE 0 END +
            CASE WHEN COALESCE(r.std_ten, 10) < 5 THEN 1 ELSE 0 END
        ) >= 2 THEN
            CASE WHEN COALESCE(rc.surface_type, 'Turf') = 'Turf' THEN 'Chaos_Turf' ELSE 'Chaos_Dirt' END
        ELSE
            CASE WHEN COALESCE(rc.surface_type, 'Turf') = 'Turf' THEN 'Core_Turf' ELSE 'Core_Dirt' END
    END AS regime
FROM kyi_base k
JOIN race_stats r
    ON r.race_date = k.race_date AND r.venue_code = k.venue_code AND r.race_no = k.race_no_str
LEFT JOIN race_cond_dedup rc
    ON rc.race_date = k.race_date AND rc.venue_code = k.venue_code AND rc.race_no = k.race_no_str
LEFT JOIN tyb_agg t
    ON t.race_date = k.race_date AND t.race_key = k.race_key AND t.umaban::text = k.umaban
LEFT JOIN ukc_agg uc
    ON uc.race_date = k.race_date AND uc.race_key = k.race_key
LEFT JOIN jvd_agg j
    ON j.race_date = k.race_date AND j.venue_code = k.venue_code
LEFT JOIN taketube_agg tt
    ON tt.race_date = k.race_date AND tt.venue_code = k.venue_code AND tt.race_no = k.race_no_str
LEFT JOIN vg_data vg
    ON k.race_date = replace(vg.vg_date::text, '/', '')
    AND k.race_no_str = lpad(vg.race_no::text, 2, '0')
    AND k.umaban = lpad(vg.umaban::text, 2, '0')
