WITH raw AS (
    SELECT * FROM {{ source('jrdb_raw', 'tyb') }}
)
SELECT
    "場コード" || "年" || "回" || "日" || "Ｒ" AS jrdb_race_key,
    "馬番" AS horse_number,
    CAST(NULLIF("ＩＤＭ", '') AS FLOAT) AS idm_before,
    CAST(NULLIF("騎手指数", '') AS FLOAT) AS jockey_index_before,
    CAST(NULLIF("情報指数", '') AS FLOAT) AS info_index_before,
    "パドック印" AS paddock_mark,
    "直前総合印" AS overall_mark_before
FROM raw
