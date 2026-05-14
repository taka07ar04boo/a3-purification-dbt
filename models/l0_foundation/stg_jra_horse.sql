-- dbt/models/l0_foundation/stg_jra_horse.sql
-- JRA-VAN 馬簿 (Staging)

{{ config(enabled=true) }}

WITH source AS (
    SELECT * FROM {{ source('public', 'jvd_um') }}
),
renamed AS (
    SELECT
        ketto_toroku_bango as horse_id,
        bamei as horse_name,
        seibetsu_code,
        moshoku_code,
        ketto_joho_01a as sire_id,
        ketto_joho_02a as dam_id,
        -- [NEW] 調教師コードをステージングに追加
        chokyoshi_code,
        seinengappi
    FROM source
)
SELECT * FROM renamed
