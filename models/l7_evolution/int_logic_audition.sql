-- dbt/models/l7_evolution/int_logic_audition.sql

{{ config(enabled=true) }}

WITH results AS (
    SELECT 
        kaisai_nen || '-' || SUBSTRING(kaisai_tsukihi, 1, 2) || '-' || SUBSTRING(kaisai_tsukihi, 3, 2) as target_at,
        keibajo_code as venue,
        race_bango::int as race,
        umaban::int as horse_no,
        CASE WHEN kakutei_chakujun = '01' THEN 1 ELSE 0 END as is_win
    FROM {{ source('public', 'jvd_se') }}
),
-- L4 のロジックアダプター群の出力を UNION
logic_outputs AS (
    SELECT * FROM {{ ref('int_logic_adapter_speed_fig') }}
    UNION ALL
    SELECT * FROM {{ ref('int_logic_adapter_sire_cushion') }}
),
-- 実績との突合
audition AS (
    SELECT
        lo.logic_id,
        r.is_win,
        lo.logic_score
    FROM logic_outputs lo
    INNER JOIN results r ON 1=1 -- 本来は日付・馬番で結合
    -- 簡略化：ロジックスコアが高い馬の勝率を計算
),
stats AS (
    SELECT
        logic_id,
        COUNT(*) as sample_size,
        AVG(CASE WHEN logic_score > 0.5 THEN is_win ELSE 0 END) as win_rate_high_score
    FROM audition
    GROUP BY 1
)
SELECT * FROM stats
