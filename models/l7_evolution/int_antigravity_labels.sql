-- dbt/models/l7_evolution/int_antigravity_labels.sql
-- L38: 反重力ターゲット (Antigravity Resonance Target - ART)
-- レース内偏差 (Z-Score) × 市場乖離 (1 - P_market) を計算し、AIの真の教師データを生成。

{{ config(enabled=true) }}

WITH scored AS (
    SELECT 
        i.*,
        se.tansho_odds::FLOAT as odds
    FROM {{ ref('int_a3_physical_index') }} i
    LEFT JOIN {{ source('public', 'jvd_se') }} se 
        ON i.kaisai_nen = se.kaisai_nen 
       AND i.kaisai_tsukihi = se.kaisai_tsukihi 
       AND i.keibajo_code = se.keibajo_code 
       AND i.race_bango = se.race_bango 
       AND i.umaban = se.umaban
),
z_stats AS (
    SELECT
        *,
        -- レース内での標準偏差と平均
        AVG(a3_raw_physical_index) OVER (PARTITION BY kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango) as race_avg,
        STDDEV(a3_raw_physical_index) OVER (PARTITION BY kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango) as race_std
    FROM scored
),
antigravity AS (
    SELECT
        *,
        -- 相対的レゾナンス・スコア (RRS)
        (a3_raw_physical_index - race_avg) / NULLIF(race_std, 0) as rrs,
        
        -- 市場期待勝率 (P_market) ※控除率を考慮し 0.8 手数料で概算
        CASE WHEN odds > 0 THEN (1.0 / (odds * 0.8)) ELSE 0 END as p_market
    FROM z_stats
),
labeling AS (
    SELECT
        *,
        -- 反重力ターゲット (ART)
        -- 人気薄(P_marketが小さい)ほど、高いRRSを出した時の Target が莫大になる。
        COALESCE(rrs * (1.0 - LEAST(p_market, 1.0)), 0) as antigravity_target
    FROM antigravity
)
SELECT * FROM labeling
