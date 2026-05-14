WITH raw AS (
    SELECT * FROM {{ source('jrdb_raw', 'kyi') }}
)
SELECT
    race_date,
    "場コード" || "年" || "回" || "日" || "Ｒ" AS jrdb_race_key,
    "馬番" AS horse_number,
    CASE 
        WHEN TRIM("ＩＤＭ") ~ '^-?[0-9]+(\.[0-9]+)?$' THEN TRIM("ＩＤＭ")::FLOAT
        WHEN TRIM("ＩＤＭ") ~ '^[0-9]+(\.[0-9]+)?-$' THEN ('-' || REPLACE(TRIM("ＩＤＭ"), '-', ''))::FLOAT
        ELSE NULL 
    END AS idm,
    CASE 
        WHEN TRIM("騎手指数") ~ '^-?[0-9]+(\.[0-9]+)?$' THEN TRIM("騎手指数")::FLOAT
        WHEN TRIM("騎手指数") ~ '^[0-9]+(\.[0-9]+)?-$' THEN ('-' || REPLACE(TRIM("騎手指数"), '-', ''))::FLOAT
        ELSE NULL 
    END AS jockey_index,
    CASE 
        WHEN TRIM("情報指数") ~ '^-?[0-9]+(\.[0-9]+)?$' THEN TRIM("情報指数")::FLOAT
        WHEN TRIM("情報指数") ~ '^[0-9]+(\.[0-9]+)?-$' THEN ('-' || REPLACE(TRIM("情報指数"), '-', ''))::FLOAT
        ELSE NULL 
    END AS info_index,
    "総合指数" AS total_index,
    "脚質" AS running_style,
    CASE 
        WHEN TRIM("idx_ten") ~ '^-?[0-9]+(\.[0-9]+)?$' THEN TRIM("idx_ten")::FLOAT
        WHEN TRIM("idx_ten") ~ '^[0-9]+(\.[0-9]+)?-$' THEN ('-' || REPLACE(TRIM("idx_ten"), '-', ''))::FLOAT
        ELSE NULL 
    END AS idx_ten,
    CASE 
        WHEN TRIM("idx_agari") ~ '^-?[0-9]+(\.[0-9]+)?$' THEN TRIM("idx_agari")::FLOAT
        WHEN TRIM("idx_agari") ~ '^[0-9]+(\.[0-9]+)?-$' THEN ('-' || REPLACE(TRIM("idx_agari"), '-', ''))::FLOAT
        ELSE NULL 
    END AS idx_agari,
    CASE 
        WHEN TRIM("idx_pace") ~ '^-?[0-9]+(\.[0-9]+)?$' THEN TRIM("idx_pace")::FLOAT
        WHEN TRIM("idx_pace") ~ '^[0-9]+(\.[0-9]+)?-$' THEN ('-' || REPLACE(TRIM("idx_pace"), '-', ''))::FLOAT
        ELSE NULL 
    END AS idx_pace,
    CASE 
        WHEN TRIM("ペース指数") ~ '^-?[0-9]+(\.[0-9]+)?$' THEN TRIM("ペース指数")::FLOAT
        WHEN TRIM("ペース指数") ~ '^[0-9]+(\.[0-9]+)?-$' THEN ('-' || REPLACE(TRIM("ペース指数"), '-', ''))::FLOAT
        ELSE NULL 
    END AS pace_index
FROM raw
