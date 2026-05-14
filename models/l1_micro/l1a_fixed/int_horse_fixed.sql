-- dbt/models/l1_micro/l1a_fixed/int_horse_fixed.sql
-- L1: 馬の不変属性 (ID, 血統, 調教師固定)
-- 後の第35層(遺伝)および第36層(調教師ハック)のための基盤。

{{ config(enabled=true) }}

WITH horses AS (
    SELECT * FROM {{ ref('stg_jra_horse') }}
),
fixed_traits AS (
    SELECT
        horse_id,
        horse_name,
        REGEXP_REPLACE(horse_name, '[[:space:]　]', '', 'g') as clean_horse_name,
        seibetsu_code,
        moshoku_code,
        sire_id,
        dam_id,
        -- [NEW] 調教師コードを追加
        chokyoshi_code as trainer_id,
        1.0 as latent_potential_base
    FROM horses
)
SELECT * FROM fixed_traits
