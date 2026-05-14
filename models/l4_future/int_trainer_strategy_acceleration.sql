-- dbt/models/l4_future/int_trainer_strategy_acceleration.sql
-- L4: 調教師戦略ハック (Trainer Strategic Intent)
-- 特定の調教パターンと「加速ラップ」の相関。意図的な「勝負のサイン」を数値化。

{{ config(enabled=true) }}

WITH trainer_traits AS (
    SELECT trainer_id, horse_id FROM {{ ref('int_horse_fixed') }}
),
training_logs AS (
    -- 最新の調教ログから「強さ」や「内容」を抽出
    SELECT 
        -- [FIX]: 上流 stg_jra_training で既にリネーム済みのため修正
        horse_id,
        -- [FUTURE PROTOTYPE]: training_at 等の利用は今後の拡張用
        horse_id as join_key
    FROM {{ ref('stg_jra_training') }}
),
historical_acceleration AS (
    -- 調教師ごとの「加速ラップ成功率」を全履歴から計算
    SELECT
        se.chokyoshi_code as trainer_id,
        COUNT(*) as total_runs,
        SUM(CASE WHEN CAST(NULLIF(NULLIF(se.kohan_3f, '0'), '') AS FLOAT) < 35.0 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0) as high_accelerator_rate
    FROM public.jvd_se se
    GROUP BY 1
),
trainer_momentum AS (
    SELECT
        tt.trainer_id,
        tt.horse_id,
        ha.high_accelerator_rate as trainer_potential,
        -- 特定の調教と、過去の加速率の掛け合わせ（仮説：調教師の得意パターン）
        RANK() OVER (ORDER BY ha.high_accelerator_rate DESC) as trainer_momentum_rank
    FROM trainer_traits tt
    JOIN historical_acceleration ha ON tt.trainer_id = ha.trainer_id
)
SELECT
    trainer_id,
    horse_id,
    trainer_potential,
    trainer_momentum_rank,
    -- 指数: 調教師が「今回、加速させる準備ができているか」の期待値
    (trainer_potential * 100.0) as trainer_intent_score
FROM trainer_momentum
