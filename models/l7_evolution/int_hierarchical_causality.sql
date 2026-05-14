-- C:\Users\新井貴士\.gemini\antigravity\scratch\dbt\models\l7_evolution\int_hierarchical_causality.sql
{{ config(materialized='table') }}

WITH base_stats AS (
    SELECT
        chokyoshi_code as trainer_code,
        COUNT(*) as total_runs,
        SUM(CASE WHEN kakutei_chakujun = '01' THEN 1 ELSE 0 END) as win_count,
        SUM(CASE WHEN kakutei_chakujun <= '03' THEN 1 ELSE 0 END) as place_count
    FROM {{ source('public', 'jvd_se') }}
    WHERE chokyoshi_code IS NOT NULL
      AND chokyoshi_code != '00000'
    GROUP BY chokyoshi_code
),

global_stats AS (
    SELECT
        SUM(win_count)::float / SUM(total_runs) as global_win_rate,
        SUM(place_count)::float / SUM(total_runs) as global_place_rate,
        -- K parameter for shrinkage (Empirical Bayes heuristic)
        10 as k_factor 
    FROM base_stats
),

hierarchical_alpha AS (
    SELECT
        b.trainer_code,
        b.total_runs,
        b.win_count,
        b.place_count,
        -- Hierarchical Win Rate (Shrinkage towards mean)
        (b.win_count + g.k_factor * g.global_win_rate) / (b.total_runs + g.k_factor) as shrunk_win_rate,
        -- Hierarchical Place Rate
        (b.place_count + g.k_factor * g.global_place_rate) / (b.total_runs + g.k_factor) as shrunk_place_rate,
        g.global_win_rate,
        g.global_place_rate
    FROM base_stats b
    CROSS JOIN global_stats g
)

SELECT
    trainer_code,
    total_runs,
    shrunk_win_rate,
    shrunk_place_rate,
    -- Stable Baseline Alpha: Deviation from global mean
    (shrunk_win_rate - global_win_rate) as stable_win_alpha,
    (shrunk_place_rate - global_place_rate) as stable_place_alpha
FROM hierarchical_alpha
