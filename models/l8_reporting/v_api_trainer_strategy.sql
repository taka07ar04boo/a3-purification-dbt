{{
    config(
        materialized='view',
        schema='public'
    )
}}

 SELECT NULL::text AS trainer_name,
    NULL::double precision AS strategy_score,
    NULL::text AS strategy_type,
    NULL::date AS date
  WHERE 1 = 0