-- models/l6_strategy/int_v2_optimizer.sql
-- ============================================================
-- Layer 4 Optimizer (B-7): EV-based Portfolio Construction
-- ============================================================
-- 推論ログのmeta_scoreと過去配当実績からEV(期待値)を推定し、
-- Kelly Criterion + 日額制約に基づく最適ベットサイジングを実施。
-- 
-- V1互換: a3_inference_logs.meta_score (0-10000スケール) を使用
-- V2拡張: detail JSONカラムが追加されたら calibrated_score 切替
-- 
-- 入力: api.a3_inference_logs, api.a3_payouts_v91, api.a3_payout_results
-- 出力: ポートフォリオ候補 (a3_portfolios INSERT可能な形式)
-- ============================================================

{{ config(
    materialized='view',
    schema='api'
) }}

WITH inference_latest AS (
    -- 直近の推論結果（V1ドライバの出力形式に準拠）
    SELECT
        il.log_id,
        il.target_date,
        il.venue_code,
        il.race_no,
        il.umaban,
        il.regime,
        il.meta_score,
        il.score_cbrank,
        il.score_rot,
        il.score_upset
    FROM {{ source('api', 'a3_inference_logs') }} il
),

-- (payout_historical CTE removed: a3_portfolios has no bet_type column.
--  All payout stats come from a3_payouts_v91 via payout_stats_real below.)

-- 実際の配当統計は a3_payouts_v91 から直接取得
payout_stats_real AS (
    SELECT
        bet_type AS strategy,
        AVG(payout) AS avg_payout,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY payout) AS median_payout,
        COUNT(*) AS sample_count
    FROM {{ source('api', 'a3_payouts_v91') }}
    GROUP BY bet_type
),

-- デフォルト配当倍率（payout_stats_realのフォールバック）
default_payouts AS (
    SELECT * FROM (VALUES
        ('WIN',     800.0, 500.0),
        ('PLACE',   250.0, 200.0),
        ('UMAREN', 3000.0, 1500.0),
        ('UMATAN', 8000.0, 3000.0),
        ('WIDE',    600.0, 400.0),
        ('WAKUREN',1500.0, 800.0)
    ) AS t(strategy, default_avg, default_median)
),

-- 配当倍率の確定
effective_payouts AS (
    SELECT
        d.strategy,
        COALESCE(
            CASE WHEN ps.sample_count >= 20 THEN ps.avg_payout ELSE NULL END,
            d.default_avg
        ) AS avg_payout,
        COALESCE(
            CASE WHEN ps.sample_count >= 20 THEN ps.median_payout ELSE NULL END,
            d.default_median
        ) AS median_payout
    FROM default_payouts d
    LEFT JOIN payout_stats_real ps ON ps.strategy = d.strategy
),

-- レース内ランキングと馬券種選択
-- V1形式: meta_score (0-10000) をそのまま信号強度として使用
-- 馬券種の選択はレース内のmeta_score順位に基づく
race_rankings AS (
    SELECT
        i.*,
        ROW_NUMBER() OVER (PARTITION BY i.target_date, i.venue_code, i.race_no ORDER BY i.meta_score DESC) AS rank_in_race,
        COUNT(*) OVER (PARTITION BY i.target_date, i.venue_code, i.race_no) AS runners_in_race,
        -- レース内での相対的な信号強度
        i.meta_score AS signal_strength,
        -- meta_scoreを確率に変換 (sigmoid-like: score/10000)
        i.meta_score / 10000.0 AS est_prob
    FROM inference_latest i
),

-- 馬券種の決定ロジック (DESIGN.md §13 準拠: 6独立馬券種)
-- V1形式ではcalibrated_scoreがないため、meta_scoreの順位で代替
strategy_selection AS (
    SELECT
        rr.*,
        CASE
            -- 1位馬: 単勝が最有力
            WHEN rr.rank_in_race = 1 AND rr.signal_strength >= 8000 THEN 'WIN'
            -- 2位馬: 馬連/馬単候補
            WHEN rr.rank_in_race = 2 AND rr.signal_strength >= 8000 THEN 'UMAREN'
            -- 3位馬: ワイド/複勝候補
            WHEN rr.rank_in_race = 3 AND rr.signal_strength >= 7000 THEN 'WIDE'
            -- score_upsetが高い (穴馬検出): 複勝 (リスクヘッジ)
            WHEN rr.score_upset > 0.3 AND rr.signal_strength >= 6000 THEN 'PLACE'
            ELSE NULL  -- 見送り
        END AS selected_strategy
    FROM race_rankings rr
),

-- EV計算 + Kelly Criterion
ev_calculation AS (
    SELECT
        ss.*,
        ep.avg_payout,
        ep.median_payout,
        -- EV per 100yen = p * (avg_payout/100) - (1-p)
        ss.est_prob * (ep.avg_payout / 100.0) - (1 - ss.est_prob) AS ev_ratio,
        -- Kelly: f = (p*b - q) / b where b = payout/100 - 1
        CASE 
            WHEN ep.avg_payout > 100 AND ss.est_prob > 0 THEN
                GREATEST(0,
                    (ss.est_prob * (ep.avg_payout / 100.0 - 1) - (1 - ss.est_prob)) 
                    / (ep.avg_payout / 100.0 - 1)
                )
            ELSE 0
        END AS kelly_fraction
    FROM strategy_selection ss
    LEFT JOIN effective_payouts ep ON ep.strategy = ss.selected_strategy
    WHERE ss.selected_strategy IS NOT NULL
),

-- ベット金額の計算
bet_sizing AS (
    SELECT
        ev.*,
        -- 信号強度ベースの金額傾斜 (DESIGN.md準拠)
        CASE
            WHEN ev.signal_strength >= 10000 THEN 300
            WHEN ev.signal_strength >= 9000 THEN 200
            WHEN ev.signal_strength >= 8000 THEN 100
            ELSE 0
        END AS signal_bet,
        -- Kelly-based (bankroll ¥50,000, half-Kelly, 100円単位)
        LEAST(
            GREATEST(
                100,
                ROUND(50000 * ev.kelly_fraction * 0.5 / 100.0) * 100
            ),
            500  -- 1レース最大500円
        ) AS kelly_bet,
        -- 最終ベット額
        CASE
            WHEN ev.ev_ratio <= 0 THEN 0  -- EV < 0 は見送り
            WHEN ev.signal_strength < 8000 THEN 0  -- 低信号は見送り
            ELSE GREATEST(100, LEAST(
                CASE
                    WHEN ev.signal_strength >= 10000 THEN 300
                    WHEN ev.signal_strength >= 9000 THEN 200
                    ELSE 100
                END,
                LEAST(
                    GREATEST(100, ROUND(50000 * ev.kelly_fraction * 0.5 / 100.0) * 100),
                    500
                )
            ))
        END AS final_bet_amount
    FROM ev_calculation ev
),

-- 日額制約の適用
daily_budget AS (
    SELECT
        bs.*,
        SUM(bs.final_bet_amount) OVER (
            PARTITION BY bs.target_date 
            ORDER BY bs.ev_ratio DESC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_daily_bet
    FROM bet_sizing bs
    WHERE bs.final_bet_amount > 0
)

-- 最終出力: ポートフォリオ候補
SELECT
    db.target_date,
    db.venue_code AS venue,
    db.race_no,
    db.umaban,
    db.selected_strategy AS strategy,
    CASE 
        WHEN db.cumulative_daily_bet <= 7000 THEN db.final_bet_amount
        WHEN db.cumulative_daily_bet - db.final_bet_amount < 5000 THEN 
            GREATEST(100, (7000 - (db.cumulative_daily_bet - db.final_bet_amount))::int)
        ELSE 0
    END AS bet_amount,
    db.ev_ratio,
    db.kelly_fraction,
    db.signal_strength,
    db.meta_score,
    db.regime,
    db.rank_in_race,
    db.score_upset,
    db.avg_payout AS expected_avg_payout,
    'PENDING'::text AS status
FROM daily_budget db
WHERE 
    db.cumulative_daily_bet - db.final_bet_amount < 7000
    AND db.final_bet_amount > 0
ORDER BY db.target_date, db.ev_ratio DESC
