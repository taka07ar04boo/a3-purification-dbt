{{
    config(
        materialized='view',
        schema='public'
    )
}}

 SELECT NULL::text AS race_key,
    NULL::integer AS horse_no,
    NULL::double precision AS odds,
    NULL::date AS date
  WHERE 1 = 0