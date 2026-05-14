-- models/intermediate/int_ssi_learning.sql

WITH results AS (
    SELECT
        kaisai_nen || '-' || SUBSTRING(kaisai_tsukihi, 1, 2) || '-' || SUBSTRING(kaisai_tsukihi, 3, 2) as race_date_str,
        keibajo_code as venue,
        race_bango::int as race,
        umaban::int as horse_no,
        CASE WHEN kakutei_chakujun = '01' THEN 1 ELSE 0 END as is_win
    FROM {{ source('public', 'jvd_se') }}
),
features AS (
    SELECT * FROM {{ ref('stg_ne_rankings') }}
),
joined AS (
    SELECT
        f.*,
        r.is_win
    FROM features f
    INNER JOIN results r ON 
        f.target_at = r.race_date_str 
        AND f.venue = r.venue 
        AND f.race = r.race 
        AND f.horse_no = r.horse_no
),
yusens_stats AS (
    SELECT
        'yusens_rank' as feature_name,
        yusens_rank::text as feature_value,
        COUNT(*) as total_count,
        SUM(is_win) as win_count,
        SUM(is_win)::float / NULLIF(COUNT(*), 0) as ssi_score
    FROM joined
    GROUP BY 1, 2
),
fu2_stats AS (
    SELECT
        'has_fu2' as feature_name,
        has_fu2::text as feature_value,
        COUNT(*) as total_count,
        SUM(is_win) as win_count,
        SUM(is_win)::float / NULLIF(COUNT(*), 0) as ssi_score
    FROM joined
    GROUP BY 1, 2
)
SELECT * FROM yusens_stats
UNION ALL
SELECT * FROM fu2_stats
