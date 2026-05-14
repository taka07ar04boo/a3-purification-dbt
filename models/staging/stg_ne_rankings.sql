-- models/staging/stg_ne_rankings.sql
-- Victgrab (NE) 当日順位データの型変換とクリーンアップ

WITH source AS (
    SELECT * FROM {{ source('public', 'ne') }}
),
renamed AS (
    SELECT
        REPLACE("年月日", '/', '-') as target_at,
        CASE "場所"
            WHEN '札幌' THEN '01'
            WHEN '函館' THEN '02'
            WHEN '福島' THEN '03'
            WHEN '新潟' THEN '04'
            WHEN '東京' THEN '05'
            WHEN '中山' THEN '06'
            WHEN '中京' THEN '07'
            WHEN '京都' THEN '08'
            WHEN '阪神' THEN '09'
            WHEN '小倉' THEN '10'
            ELSE "場所" -- 万が一のために維持
        END as venue,
        "Ｒ"::INT as race,
        "枠番"::INT as wakuban,
        "馬番"::INT as horse_no,
        "馬名" as horse_name,
        -- 優先値 (すでには integer)
        COALESCE("優先値", 0) as yusens,
        -- 優先値順 (すでに integer)
        COALESCE("優先値順", 99) as yusens_rank,
        -- FU2 (すでに integer)
        CASE 
            WHEN "fu2" IS NOT NULL AND "fu2" > 0 THEN 1 
            ELSE 0 
        END as has_fu2,
        -- F指数 (すでに integer)
        COALESCE("Ｆ指数", 0) as f_index,
        -- R印
        CASE WHEN "r印1" IS NOT NULL AND "r印1" NOT IN ('', '-', 'None') THEN 1 ELSE 0 END as has_r1,
        CASE WHEN "r印2" IS NOT NULL AND "r印2" NOT IN ('', '-', 'None') THEN 1 ELSE 0 END as has_r2
    FROM source
    WHERE "年月日" IS NOT NULL
)
SELECT * FROM renamed
