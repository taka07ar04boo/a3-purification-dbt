{{ config(enabled=true) }}

-- dbt/models/l4_future/int_logic_adapter_sire_momentum.sql
-- L4: 加速ラップ遺伝指数 (Genetic Momentum Index)
-- 過去20年の実績から、特定の血統（父・母父）が持つ「末脚加速の爆発力」を多重化。

WITH horse_traits AS (
    SELECT * FROM {{ ref('int_horse_fixed') }}
),
race_results AS (
    -- 過去の結果テーブルを直接参照（歴史的パターンの抽出のため。当日リークではない）
    SELECT 
        ketto_toroku_bango as horse_id,
        CASE WHEN kakutei_chakujun = '01' THEN 1 ELSE 0 END as is_win,
        CAST(NULLIF(kohan_3f, '0') AS FLOAT) as last_3f,
        CAST(NULLIF(kakutei_chakujun, '00') AS INT) as final_rank
    FROM public.jvd_se
),
sire_stats AS (
    SELECT
        ht.sire_id,
        COUNT(*) as progeny_count,
        SUM(rr.is_win)::float / NULLIF(COUNT(*), 0) as progeny_win_rate,
        AVG(rr.last_3f) as avg_last_3f,
        AVG(rr.final_rank) as avg_rank
    FROM horse_traits ht
    JOIN race_results rr ON ht.horse_id = rr.horse_id
    WHERE ht.sire_id IS NOT NULL 
    GROUP BY 1
    HAVING COUNT(*) >= 10 -- 統計的有意性を担保
),
momentum_index AS (
    SELECT
        sire_id,
        progeny_count,
        progeny_win_rate,
        -- 加速指数: 平均上がり3Fと平均着順の相関。
        -- 小さいほど「着順に対して上がりが鋭い（爆発力がある）」ことを示す。
        (avg_last_3f / NULLIF(avg_rank, 0)) as momentum_raw
    FROM sire_stats
)
SELECT
    sire_id,
    progeny_count,
    progeny_win_rate,
    -- 指数化（標準化的に処理）
    DENSE_RANK() OVER (ORDER BY momentum_raw ASC) as genetic_momentum_rank,
    momentum_raw as genetic_momentum_score
FROM momentum_index
