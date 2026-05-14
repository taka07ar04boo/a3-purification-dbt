{{ config(materialized='table', schema='public') }}

{{ config(enabled=false) }}

WITH scored_data AS (
    SELECT 
        s.target_at,
        s.venue,
        s.race,
        s.horse_no,
        s.a3_causal_intelligence_score as score
    FROM {{ ref('int_a3_intelligence_final_score') }} s
),
race_outcomes AS (
    SELECT 
        target_date,
        venue_code,
        SUBSTRING(race_key FROM 7 FOR 2)::int as race_no_int,
        horse_no,
        track_type,
        class_code,
        distance,
        actual_rank::int as actual_rank,
        win_odds_raw::numeric / 10.0 as win_odds
    FROM {{ ref('stg_jrdb_sed') }}
),
joined_results AS (
    SELECT 
        s.*,
        a.track_type,
        a.class_code,
        a.distance,
        a.venue_code,
        a.win_odds,
        a.actual_rank,
        CASE WHEN a.actual_rank = 1 THEN 1 ELSE 0 END as is_win,
        CASE WHEN a.actual_rank <= 3 THEN 1 ELSE 0 END as is_place,
        CASE WHEN a.actual_rank = 1 THEN a.win_odds ELSE 0 END as win_payout
    FROM scored_data s
    INNER JOIN race_outcomes a 
      ON s.target_at::date = a.target_date
      AND s.venue = a.venue_code
      AND s.race::int = a.race_no_int
      AND s.horse_no = a.horse_no
)

SELECT 
    track_type,
    class_code,
    venue_code,
    width_bucket(distance::int, 0, 4000, 4) * 1000 as distance_bracket,
    width_bucket(score, 0, 100, 10) * 10 as score_bracket,
    COUNT(*) as total_horses,
    SUM(is_win) as wins,
    SUM(is_place) as places,
    ROUND(AVG(is_win)::numeric, 4) as win_rate,
    ROUND(AVG(is_place)::numeric, 4) as place_rate,
    ROUND(AVG(win_payout)::numeric, 4) as win_roi,
    ROUND(AVG(score)::numeric, 2) as avg_score
FROM joined_results
GROUP BY 
    track_type, 
    class_code, 
    venue_code, 
    distance_bracket, 
    score_bracket
ORDER BY 
    score_bracket DESC
