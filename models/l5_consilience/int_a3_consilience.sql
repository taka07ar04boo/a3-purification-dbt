{{ config(enabled=true) }}

-- dbt/models/l5_consilience/int_a3_consilience.sql
-- L5: 統合・共鳴 (Meta-Consilience)
-- [EVOLUTION]: 物理 × 意図 × 遺伝 × シナジー × 因果(Uplift)
-- 五重の共鳴スリットにより、期待値の歪みを物理的・因果的に特定する。

WITH features AS (
    SELECT * FROM {{ ref('int_a3_consis_features') }}
),
anomalies AS (
    SELECT * FROM {{ ref('int_a3_consis_anomalies') }}
),
-- [RESTORED] JRDB Genome Intelligence
jrdb_genome AS (
    SELECT 
        "場コード" || "年" || "回" || "日" || "Ｒ" AS jrdb_race_key,
        CASE WHEN "馬番" ~ '^[0-9]+$' THEN "馬番"::int ELSE NULL END AS horse_no,
        CASE WHEN "ＩＤＭ" ~ '^-?[0-9.]+$' THEN "ＩＤＭ"::float ELSE NULL END AS idm,
        "脚質" AS running_style,
        1.0 as joint_ground_aptitude
    FROM {{ source('jrdb_raw', 'kyi') }}
),
-- [EVOLUTION L35/L36] 遺伝 & 意図
sire_momentum AS ( SELECT * FROM {{ ref('int_logic_adapter_sire_momentum') }} ),
trainer_intent AS ( SELECT * FROM {{ ref('int_trainer_strategy_acceleration') }} ),
-- [EVOLUTION L41] シナジー
synergy AS ( SELECT * FROM {{ ref('int_jockey_trainer_synergy') }} ),
-- [EVOLUTION L42] 因果アップリフト (NEW)
uplift AS ( SELECT * FROM {{ ref('int_antigravity_uplift') }} ),

combined AS (
    SELECT
        f.*,
        a.horse_id as anomaly_horse_id,
        a.prev_rank, a.prev_pop, a.prev_c4_pos, a.prev_weight, a.prev_distance, a.prev_race_date,
        a.win_rate, a.place_rate, a.real_koso_rate,
        a.phys_type, a.hoof_code,
        
        jg.idm,
        jg.running_style,
        jg.joint_ground_aptitude,
        
        sm.genetic_momentum_score,
        ti.trainer_intent_score,
        sy.synergy_reliability_score,
        COALESCE(u.uplift_resonance_score, 0) as uplift_score,
        
        -- [CONSILIENCE SCORE LOGIC - CAUSAL RECONSTRUCTION]
        (
            -- 1. 物理 & 異常 (L1-L4)
            (CASE WHEN f.immediate_form_score > 10 THEN 5 ELSE 0 END) +
            (CASE WHEN a.prev_pop <= 2 AND a.prev_rank >= 10 AND a.prev_c4_pos >= 12 THEN 20 ELSE 0 END) +
            
            -- 2. 三重共鳴 (DNA × INTENT × SYNERGY)
            (CASE WHEN sm.genetic_momentum_score < 5.0 THEN 10 ELSE 0 END) + 
            (CASE WHEN ti.trainer_intent_score > 70 THEN 10 ELSE 0 END) +    
            (CASE WHEN sy.synergy_reliability_score > 40 THEN 10 ELSE 0 END) + 
            
            -- 3. [CAUSAL UPLIFT] 因果アルファ
            -- 特定の介入（コンビ）が、物理的期待値を「押し上げている（Uplift）」点への最大ボーナス
            (CASE WHEN u.uplift_resonance_score > 10 THEN 25 ELSE 0 END) +
            
            -- 4. 【共鳴の極致】物理(A3) × シナジー(Human) × 因果(Uplift) の三位一体
            (CASE WHEN sy.synergy_reliability_score > 40 AND u.uplift_resonance_score > 10 THEN 30 ELSE 0 END)
            
        ) as consilience_score
    FROM features f
    LEFT JOIN anomalies a ON f.target_at = a.target_at AND f.venue = a.venue AND f.race = a.race AND f.horse_no = a.horse_no
    -- 結合
    LEFT JOIN jrdb_genome jg ON f.venue = jg.jrdb_race_key AND f.horse_no = jg.horse_no
    LEFT JOIN {{ ref('int_horse_fixed') }} hf ON f.clean_horse_name = hf.clean_horse_name
    LEFT JOIN sire_momentum sm ON hf.sire_id = sm.sire_id
    LEFT JOIN trainer_intent ti ON hf.horse_id = ti.horse_id
    LEFT JOIN synergy sy ON f.chokyo_code = sy.trainer_code
    -- Uplift結合
    LEFT JOIN uplift u ON f.target_at = u.target_at AND f.venue = u.keibajo_code AND f.race = CAST(u.race_bango AS INTEGER) AND f.horse_no = CAST(u.umaban AS INTEGER)
)
SELECT * FROM combined
