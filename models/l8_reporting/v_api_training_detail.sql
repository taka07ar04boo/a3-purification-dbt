{{
    config(
        materialized='view',
        schema='public'
    )
}}

 SELECT NULL::text AS horse_name,
    NULL::double precision AS training_index,
    NULL::integer AS is_accelerating,
    NULL::integer AS days_since,
    NULL::double precision AS stable_rank,
    NULL::text AS course_type,
    NULL::text AS farm_rank,
    NULL::date AS date
  WHERE 1 = 0