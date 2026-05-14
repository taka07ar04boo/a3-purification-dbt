-- dbt/models/l4_external/int_external_opinions.sql
-- L4: 外部知能・未来知識 (External Intelligence / Future Knowledge)
-- Victgrab (NE) などの外部予測値を集約し、重み付けの対象とする層

WITH ne_source AS (
    SELECT 
        target_at,
        venue,
        race,
        horse_no,
        yusens,
        yusens_rank,
        f_index,
        has_fu2,
        has_r1,
        has_r2
    FROM {{ ref('stg_ne_rankings') }}
),
-- 将来的に他の外部予想家 (L4c, L4d) を追加する場合はここに JOIN する
final AS (
    SELECT
        *,
        -- 外部指数に基づく暫定的な「期待強度」
        (CASE WHEN has_fu2 = 1 THEN 1.5 ELSE 1.0 END) * 
        (CASE WHEN f_index > 0 THEN (f_index / 100.0) ELSE 1.0 END) as external_strength_idx
    FROM ne_source
)
SELECT * FROM final
