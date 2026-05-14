-- dbt/models/l7_evolution/int_self_audit.sql
-- L7: 自己監査 (Self-Audit)
-- 各層の予測値と実際の結果（確定着順）を突合し、性能を記録する。

{{ config(enabled=true) }}

WITH results AS (
    SELECT 
        kaisai_nen || '-' || SUBSTRING(kaisai_tsukihi, 1, 2) || '-' || SUBSTRING(kaisai_tsukihi, 3, 2) as target_at,
        keibajo_code as venue,
        race_bango::int as race,
        umaban::int as horse_no,
        ketto_toroku_bango as horse_id, -- 内部結合用
        CASE WHEN kakutei_chakujun ~ '^[0-9]+$' THEN kakutei_chakujun::int ELSE 99 END as actual_rank,
        CASE WHEN kakutei_chakujun = '01' THEN 1 ELSE 0 END as is_win
    FROM {{ source('public', 'jvd_se') }}
),
-- 層ごとの予測値を統合 (レース単位で記録されているもの)
race_level_predictions AS (
    -- L4: 未来知識 (Victgrab/NE)
    SELECT
        'L4_Victgrab' as source_layer,
        target_at, venue, race, horse_no,
        external_strength_idx as predicted_score,
        yusens_rank as predicted_rank
    FROM {{ ref('int_external_opinions') }}
    
    UNION ALL
    
    -- L5: 統合知能 (Consilience)
    SELECT
        'L5_Consilience' as source_layer,
        target_at, venue, race, horse_no,
        consilience_score as predicted_score,
        NULL::int as predicted_rank
    FROM {{ ref('int_a3_consilience') }}
),
-- 馬単位で記録されている個体知能 (L1) をレース結果に紐付け
micro_audit AS (
    SELECT
        'L1_Micro' as source_layer,
        r.target_at, r.venue, r.race, r.horse_no,
        lp.immediate_form_score as predicted_score,
        NULL::int as predicted_rank,
        r.actual_rank,
        r.is_win
    FROM results r
    LEFT JOIN {{ ref('int_horse_point_condition') }} lp ON r.horse_id = lp.horse_id
),
audit_log AS (
    -- レースレベルの監査結果
    SELECT
        p.source_layer,
        p.target_at, p.venue, p.race, p.horse_no,
        p.predicted_score, p.predicted_rank,
        r.actual_rank, r.is_win,
        ABS(COALESCE(p.predicted_rank, 99) - r.actual_rank) as rank_error
    FROM race_level_predictions p
    INNER JOIN results r ON 
        LEFT(p.target_at::text, 10) = r.target_at 
        AND p.venue = r.venue 
        AND p.race = r.race 
        AND p.horse_no = r.horse_no
    
    UNION ALL
    
    -- マイクロ知能の監査結果
    SELECT
        source_layer,
        target_at, venue, race, horse_no,
        predicted_score, predicted_rank,
        actual_rank, is_win,
        99 as rank_error -- L1は着順予測がないため
    FROM micro_audit
)
SELECT * FROM audit_log
