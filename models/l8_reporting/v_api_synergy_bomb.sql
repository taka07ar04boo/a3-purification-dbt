{{
    config(
        materialized='view',
        schema='public'
    )
}}

 SELECT NULL::text AS horse_name,
    NULL::double precision AS synergy,
    NULL::date AS date
  WHERE 1 = 0