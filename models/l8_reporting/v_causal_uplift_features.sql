{{
    config(
        materialized='view',
        schema='api'
    )
}}

 SELECT k.race_key,
    k.race_date,
    k."場コード" AS venue_code,
    k."Ｒ" AS race_no,
    k.umaban,
    k.horse_id,
        CASE
            WHEN k.jockey_code <> prev.jockey_code THEN 1
            ELSE 0
        END AS feat_jockey_change,
        CASE
            WHEN k.distance::numeric > COALESCE(prev.distance::numeric, k.distance::numeric) THEN 1
            WHEN k.distance::numeric < COALESCE(prev.distance::numeric, k.distance::numeric) THEN '-1'::integer
            ELSE 0
        END AS feat_distance_change,
    COALESCE(k.distance::numeric - prev.distance::numeric, 0::numeric) AS feat_distance_delta,
        CASE
            WHEN COALESCE(k."ローテーション"::numeric, 0::numeric) >= 90::numeric THEN 1
            ELSE 0
        END AS feat_long_rest,
        CASE
            WHEN k.class_code::numeric > COALESCE(prev.class_code::numeric, k.class_code::numeric) THEN 1
            ELSE 0
        END AS feat_class_upgrade,
        CASE
            WHEN k.track_type <> COALESCE(prev.track_type, k.track_type) THEN 1
            ELSE 0
        END AS feat_surface_change
   FROM kyi_parsed_v51 k
     LEFT JOIN LATERAL ( SELECT k2.jockey_code,
            k2.distance,
            k2.class_code,
            k2.track_type
           FROM kyi_parsed_v51 k2
          WHERE k2.horse_id = k.horse_id AND k2.race_date < k.race_date
          ORDER BY k2.race_date DESC
         LIMIT 1) prev ON true
  WHERE k.horse_id IS NOT NULL AND k.horse_id <> ''::text