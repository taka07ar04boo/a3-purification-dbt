-- dbt/models/l3_bias/int_day_track_bias.sql

WITH results AS (
    SELECT 
        kaisai_nen || '-' || SUBSTRING(kaisai_tsukihi, 1, 2) || '-' || SUBSTRING(kaisai_tsukihi, 3, 2) as target_at,
        keibajo_code as venue,
        race_bango::int as race,
        umaban::int as horse_no,
        wakuban::int as waku_no,
        -- 脚質傾向 (jvd_se の実際の商品名にあわせる必要があるが、一旦 style として定義)
        kakutei_chakujun = '01' as is_win
    FROM {{ source('public', 'jvd_se') }}
),
-- 同日の前のレースからバイアスを計算
bias_calc AS (
    SELECT
        target_at,
        venue,
        race,
        -- 内枠 (1-3) の勝率 (前のレースまでを累計)
        AVG(CASE WHEN waku_no <= 3 THEN (CASE WHEN is_win THEN 1 ELSE 0 END) ELSE 0 END) 
            OVER (PARTITION BY target_at, venue ORDER BY race ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as inner_bias
    FROM results
)
SELECT DISTINCT * FROM bias_calc
