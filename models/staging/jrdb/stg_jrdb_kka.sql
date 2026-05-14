{{ config(enabled=true) }}

WITH raw AS (
    SELECT CAST(NULL AS VARCHAR) as "場コード", CAST(NULL AS VARCHAR) as "年", CAST(NULL AS VARCHAR) as "回", CAST(NULL AS VARCHAR) as "日", CAST(NULL AS VARCHAR) as "Ｒ", CAST(NULL AS VARCHAR) as "馬番", CAST(NULL AS VARCHAR) as "ＪＲＡ成績" WHERE 1=0
)
SELECT
    "場コード" || "年" || "回" || "日" || "Ｒ" AS jrdb_race_key,
    "馬番" AS horse_number,
    "ＪＲＡ成績" AS record_jra
FROM raw
