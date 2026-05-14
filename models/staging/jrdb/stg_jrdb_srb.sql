{{ config(enabled=true) }}

WITH raw AS (
    SELECT CAST(NULL AS VARCHAR) as "場コード", CAST(NULL AS VARCHAR) as "年", CAST(NULL AS VARCHAR) as "回", CAST(NULL AS VARCHAR) as "日", CAST(NULL AS VARCHAR) as "Ｒ", CAST(NULL AS VARCHAR) as "ハロンタイム", CAST(NULL AS VARCHAR) as "１コーナー", CAST(NULL AS VARCHAR) as "２コーナー", CAST(NULL AS VARCHAR) as "３コーナー", CAST(NULL AS VARCHAR) as "４コーナー", CAST(NULL AS VARCHAR) as "レースコメント" WHERE 1=0
)
SELECT
    "場コード" || "年" || "回" || "日" || "Ｒ" AS jrdb_race_key,
    "ハロンタイム" AS lap_times,
    "１コーナー" AS corner_pos_1,
    "２コーナー" AS corner_pos_2,
    "３コーナー" AS corner_pos_3,
    "４コーナー" AS corner_pos_4,
    "レースコメント" AS race_comment
FROM raw
