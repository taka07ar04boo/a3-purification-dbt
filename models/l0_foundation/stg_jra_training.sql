-- dbt/models/l0_foundation/stg_jra_training.sql

{{ config(enabled=true) }}

WITH source AS (
    SELECT * FROM {{ source('public', 'jvd_hc') }}
),
renamed AS (
    SELECT
        ketto_toroku_bango as horse_id,
        chokyo_nengappi as training_at,
        time_gokei_4f::float / 10.0 as time_4f,
        time_gokei_3f::float / 10.0 as time_3f,
        time_gokei_2f::float / 10.0 as time_2f,
        lap_time_1f::float / 10.0 as time_1f
    FROM source
    WHERE time_gokei_4f IS NOT NULL AND time_gokei_4f != ''
)
SELECT * FROM renamed
