-- v_sed_resolved: 成績解決ビュー (旧 api.v_sed_resolved のdbt化)
-- sed_parsed (JRDB) と jvd_se (JRA-VAN) を統合、JRDB優先で重複排除
{{ config(materialized='view', schema='api') }}

WITH combined AS (
    SELECT
        "年月日" AS race_date,
        "場コード" AS venue_code,
        LEFT("年月日", 4) AS year,
        "回" AS kai,
        "日" AS nichi,
        "Ｒ" AS race_no,
        LPAD("馬番", 2, '0') AS umaban,
        "着順" AS rank,
        "頭数" AS total_horses,
        "距離" AS distance,
        CASE
            WHEN "確定単勝オッズ" IS NOT NULL AND "確定単勝オッズ" <> '' THEN "確定単勝オッズ"
            ELSE NULL
        END AS odds_win,
        CASE "芝ダ障害コード"
            WHEN '1' THEN 'Turf'
            WHEN '2' THEN 'Dirt'
            WHEN '3' THEN 'Steeple'
            ELSE ''
        END AS race_type,
        CASE "右左"
            WHEN '1' THEN 'Right'
            WHEN '2' THEN 'Left'
            WHEN '3' THEN 'Straight'
            ELSE ''
        END AS race_style,
        1 AS priority
    FROM {{ source('public', 'sed_parsed') }}
    WHERE "年月日" IS NOT NULL

    UNION ALL

    SELECT
        kaisai_nen::text || kaisai_tsukihi::text AS race_date,
        keibajo_code AS venue_code,
        kaisai_nen AS year,
        LPAD(kaisai_kai::text, 2, '0') AS kai,
        LPAD(kaisai_nichime::text, 2, '0') AS nichi,
        LPAD(race_bango::text, 2, '0') AS race_no,
        LPAD(umaban::text, 2, '0') AS umaban,
        LPAD(kakutei_chakujun::text, 2, '0') AS rank,
        COUNT(*) OVER (
            PARTITION BY kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
        )::text AS total_horses,
        NULL::text AS distance,
        CASE WHEN tansho_odds ~ '^[0-9]+(\.[0-9]+)?$' THEN (tansho_odds::numeric / 10.0)::text ELSE NULL END AS odds_win,
        NULL::text AS race_type,
        NULL::text AS race_style,
        2 AS priority
    FROM {{ source('public', 'jvd_se') }}
    WHERE kaisai_nen IS NOT NULL
),

deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY race_date, venue_code, race_no, umaban
            ORDER BY priority
        ) AS rn
    FROM combined
)

SELECT
    race_date,
    venue_code,
    year,
    kai,
    nichi,
    race_no,
    umaban,
    rank,
    total_horses,
    distance,
    odds_win,
    race_type,
    race_style
FROM deduped
WHERE rn = 1
