{{ config(
    materialized='view',
    schema='api'
) }}

-- v_bac_resolved: bac_parsed (番組データ) から主要カラムを英語名にリネーム
-- Phase 399: dbt化。レーススケジュール情報の標準化ビュー。
SELECT
    b."年月日"          AS race_date,
    b."場コード"        AS venue_code,
    lpad(b."Ｒ"::text, 2, '0') AS race_no,
    b."発走時間"        AS start_time,
    b."距離"::integer   AS distance_m,
    b."芝ダ障害コード"  AS surface_code,
    b."頭数"::integer   AS total_horses,
    b."種別"            AS race_type,
    b."条件"            AS race_condition,
    b."グレード"        AS grade,
    b."レース名"        AS race_name,
    b.race_date         AS file_race_date,
    b.race_key
FROM {{ source('public', 'bac_parsed') }} b
WHERE b."年月日" IS NOT NULL
