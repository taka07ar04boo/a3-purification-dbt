-- dbt/models/l2_context/l2d_dynamics/int_zscore_features.sql

WITH source AS (
    SELECT
        jrdb_race_key AS race_key,
        horse_number AS umaban,
        COALESCE(idm, 0) AS feat_score_idm,
        COALESCE(idx_ten, 0) AS feat_idx_ten,
        COALESCE(idx_agari, 0) AS feat_idx_agari
    FROM {{ ref('stg_jrdb_kyi') }}
),
stats AS (
    SELECT
        race_key,
        AVG(feat_score_idm) AS mean_idm,
        STDDEV_SAMP(feat_score_idm) AS std_idm,
        AVG(feat_idx_ten) AS mean_ten,
        STDDEV_SAMP(feat_idx_ten) AS std_ten,
        AVG(feat_idx_agari) AS mean_agari,
        STDDEV_SAMP(feat_idx_agari) AS std_agari
    FROM source
    GROUP BY 1
),
zscores AS (
    SELECT
        s.race_key,
        s.umaban,
        -- IDM
        st.mean_idm,
        COALESCE(st.std_idm, 0) AS feat_race_std_idm,
        CASE 
            WHEN COALESCE(st.std_idm, 0) = 0 THEN 0.0 
            ELSE (s.feat_score_idm - st.mean_idm) / st.std_idm 
        END AS feat_idm_zscore,
        -- TEN
        st.mean_ten,
        COALESCE(st.std_ten, 0) AS feat_race_std_ten,
        CASE 
            WHEN COALESCE(st.std_ten, 0) = 0 THEN 0.0 
            ELSE (s.feat_idx_ten - st.mean_ten) / st.std_ten 
        END AS feat_ten_zscore,
        -- AGARI
        st.mean_agari,
        COALESCE(st.std_agari, 0) AS feat_race_std_agari,
        CASE 
            WHEN COALESCE(st.std_agari, 0) = 0 THEN 0.0 
            ELSE (s.feat_idx_agari - st.mean_agari) / st.std_agari 
        END AS feat_agari_zscore
    FROM source s
    JOIN stats st ON s.race_key = st.race_key
)
SELECT * FROM zscores
