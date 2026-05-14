{{ config(
    materialized='view',
    schema='api'
) }}

WITH vg_data AS (
    SELECT replace(split_part((ne."年月日")::text, ' '::text, 1), '/'::text, ''::text) AS race_date,
           ne."場所" AS venue_code_name,
           (ne."Ｒ")::integer AS race_no,
           (ne."馬番")::integer AS umaban,
           ne.fu2,
           ne."優先値",
           ne."Ｆ指数"
    FROM public.ne
),
tyb_agg AS (
    SELECT tyb_parsed.race_date,
           tyb_parsed.race_key,
           tyb_parsed.umaban,
           CASE
               WHEN (TRIM(BOTH FROM tyb_parsed."パドック指数") ~ '^[-+]?[0-9]*\.?[0-9]+$'::text) THEN (TRIM(BOTH FROM tyb_parsed."パドック指数"))::numeric
               ELSE (0)::numeric
           END AS tyb_paddock_score,
           CASE
               WHEN (TRIM(BOTH FROM tyb_parsed."馬体重") ~ '^[-+]?[0-9]*\.?[0-9]+$'::text) THEN (TRIM(BOTH FROM tyb_parsed."馬体重"))::numeric
               ELSE (0)::numeric
           END AS tyb_horse_weight,
           CASE
               WHEN (TRIM(BOTH FROM tyb_parsed."馬体重増減") ~ '^[-+]?[0-9]*\.?[0-9]+$'::text) THEN (TRIM(BOTH FROM tyb_parsed."馬体重増減"))::numeric
               ELSE (0)::numeric
           END AS tyb_weight_change,
           CASE
               WHEN (TRIM(BOTH FROM tyb_parsed."気配コード") ~ '^[-+]?[0-9]*\.?[0-9]+$'::text) THEN (TRIM(BOTH FROM tyb_parsed."気配コード"))::numeric
               ELSE (0)::numeric
           END AS tyb_vibe_code,
           CASE
               WHEN (TRIM(BOTH FROM tyb_parsed."騎手指数") ~ '^[-+]?[0-9]*\.?[0-9]+$'::text) THEN (TRIM(BOTH FROM tyb_parsed."騎手指数"))::numeric
               ELSE (0)::numeric
           END AS tyb_jockey_score,
           CASE
               WHEN (TRIM(BOTH FROM tyb_parsed."情報指数") ~ '^[-+]?[0-9]*\.?[0-9]+$'::text) THEN (TRIM(BOTH FROM tyb_parsed."情報指数"))::numeric
               ELSE (0)::numeric
           END AS tyb_info_score,
           CASE
               WHEN (TRIM(BOTH FROM tyb_parsed."オッズ指数") ~ '^[-+]?[0-9]*\.?[0-9]+$'::text) THEN (TRIM(BOTH FROM tyb_parsed."オッズ指数"))::numeric
               ELSE (0)::numeric
           END AS tyb_odds_score,
           CASE
               WHEN (TRIM(BOTH FROM tyb_parsed."直前総合印") ~ '^[-+]?[0-9]*\.?[0-9]+$'::text) THEN (TRIM(BOTH FROM tyb_parsed."直前総合印"))::numeric
               ELSE (0)::numeric
           END AS tyb_cho_total_mark,
           CASE
               WHEN (TRIM(BOTH FROM tyb_parsed."ＩＤＭ") ~ '^[-+]?[0-9]*\.?[0-9]+$'::text) THEN (TRIM(BOTH FROM tyb_parsed."ＩＤＭ"))::numeric
               ELSE (0)::numeric
           END AS tyb_idm,
           CASE
               WHEN (TRIM(BOTH FROM tyb_parsed."単勝オッズ") ~ '^[-+]?[0-9]*\.?[0-9]+$'::text) THEN (TRIM(BOTH FROM tyb_parsed."単勝オッズ"))::numeric
               ELSE (0)::numeric
           END AS tyb_win_odds,
           CASE
               WHEN (TRIM(BOTH FROM tyb_parsed."複勝オッズ") ~ '^[-+]?[0-9]*\.?[0-9]+$'::text) THEN (TRIM(BOTH FROM tyb_parsed."複勝オッズ"))::numeric
               ELSE (0)::numeric
           END AS tyb_place_odds,
           CASE
               WHEN (TRIM(BOTH FROM tyb_parsed."馬具変更情報") ~ '^[-+]?[0-9]*\.?[0-9]+$'::text) THEN (TRIM(BOTH FROM tyb_parsed."馬具変更情報"))::numeric
               ELSE (0)::numeric
           END AS tyb_equipment_change,
           CASE
               WHEN (TRIM(BOTH FROM tyb_parsed."脚元情報") ~ '^[-+]?[0-9]*\.?[0-9]+$'::text) THEN (TRIM(BOTH FROM tyb_parsed."脚元情報"))::numeric
               ELSE (0)::numeric
           END AS tyb_leg_info,
           CASE
               WHEN (TRIM(BOTH FROM tyb_parsed."馬体コード") ~ '^[-+]?[0-9]*\.?[0-9]+$'::text) THEN (TRIM(BOTH FROM tyb_parsed."馬体コード"))::numeric
               ELSE (0)::numeric
           END AS tyb_body_code
    FROM public.tyb_parsed
)
SELECT v53.race_date,                                                                                                                                                                      
     v53.venue_code,                                                                                                                                                                         
     v53.race_no,                                                                                                                                                                            
     v53.umaban,                                                                                                                                                                             
     v53.horse_id,                                                                                                                                                                           
     v53.horse_name,                                                                                                                                                                         
     v53.jockey_code,                                                                                                                                                                        
     v53.trainer_code,                                                                                                                                                                       
     v53.gate_no,                                                                                                                                                                            
     v53.score_idm,                                                                                                                                                                          
     v53.idx_ten,                                                                                                                                                                            
     v53.idx_agari,                                                                                                                                                                          
     v53.odds_win_pre,                                                                                                                                                                       
     v53.pop_win_pre,                                                                                                                                                                        
     v53.distance_m,                                                                                                                                                                         
     v53.total_horses,                                                                                                                                                                       
     v53.class_code_num,                                                                                                                                                                     
     v53.is_hurdle,                                                                                                                                                                          
     v53.surface_type,                                                                                                                                                                       
     v53.idm_zscore,                                                                                                                                                                         
     v53.ten_zscore,                                                                                                                                                                         
     v53.agari_zscore,                                                                                                                                                                       
     v53.race_std_idm,                                                                                                                                                                       
     v53.race_std_ten,                                                                                                                                                                       
     v53.race_std_agari,                                                                                                                                                                     
     v53.tyb_total_score,                                                                                                                                                                    
     v53.tyb_track_cond,                                                                                                                                                                     
     v53.jvd_cushion_val,                                                                                                                                                                    
     v53.sire_system_code,                                                                                                                                                                   
     v53.bms_system_code,                                                                                                                                                                    
     v53.gekiso_index,                                                                                                                                                                       
     v53.dist_aptitude,                                                                                                                                                                      
     v53.stable_eval_code,                                                                                                                                                                   
     v53.training_arrow_code,                                                                                                                                                                
     v53.gekiso_dist_cross,                                                                                                                                                                  
     v53.training_stable_cross,                                                                                                                                                              
     v53.grass_aptitude_code,                                                                                                                                                                
     v53.dirt_aptitude_code,                                                                                                                                                                 
     v53.running_style_code,                                                                                                                                                                 
     v53.mud_aptitude_code,                                                                                                                                                                  
     v53.running_form_code,                                                                                                                                                                  
     v53.body_type_code,                                                                                                                                                                     
     v53.rotation_days,                                                                                                                                                                      
     v53.dist_aptitude2,                                                                                                                                                                     
     v53.longshot_index,                                                                                                                                                                     
     v53.start_index,                                                                                                                                                                        
     v53.late_start_rate,                                                                                                                                                                    
     v53.popular_index,                                                                                                                                                                      
     v53.jockey_exp_rentai,                                                                                                                                                                  
     v53.jockey_exp_win,                                                                                                                                                                     
     v53.jockey_exp_top3,                                                                                                                                                                    
     v53.track_fit_signal,                                                                                                                                                                   
     v53.tt_bias_score,                                                                                                                                                                      
     v53.tt_alpha_signal,                                                                                                                                                                    
     v53.tt_confidence,                                                                                                                                                                      
     v53.tt_video_count,                                                                                                                                                                     
     v53.low_pop_high_idm,                                                                                                                                                                   
     v53.pace_advantage_score,                                                                                                                                                               
     v53.actual_rank,                                                                                                                                                                        
     v53.is_winner,                                                                                                                                                                          
     v53.is_top3,                                                                                                                                                                            
     v53.rank_ratio,                                                                                                                                                                         
     v53.rotation_seg,                                                                                                                                                                       
     v53.gate_style_cross,                                                                                                                                                                   
     v53.gate_bias_score,                                                                                                                                                                    
     v53.dist_category,                                                                                                                                                                      
     v53.gate_style_dist_cross,                                                                                                                                                              
     v53.style_rotation_cross,                                                                                                                                                               
     v53.short_rest_high_idm,                                                                                                                                                                
     v53.tt_idm_cross,                                                                                                                                                                       
     COALESCE(t.tyb_paddock_score, (0)::numeric) AS tyb_paddock_score,                                                                                                                       
     COALESCE(t.tyb_horse_weight, (0)::numeric) AS tyb_horse_weight,                                                                                                                         
     COALESCE(t.tyb_weight_change, (0)::numeric) AS tyb_weight_change,                                                                                                                       
     COALESCE(t.tyb_vibe_code, (0)::numeric) AS tyb_vibe_code,                                                                                                                               
     COALESCE(t.tyb_jockey_score, (0)::numeric) AS tyb_jockey_score,                                                                                                                         
     COALESCE(t.tyb_info_score, (0)::numeric) AS tyb_info_score,                                                                                                                             
     COALESCE(t.tyb_odds_score, (0)::numeric) AS tyb_odds_score,                                                                                                                             
     COALESCE(t.tyb_cho_total_mark, (0)::numeric) AS tyb_cho_total_mark,                                                                                                                     
     COALESCE(t.tyb_idm, (0)::numeric) AS tyb_idm,                                                                                                                                           
     COALESCE(t.tyb_win_odds, (0)::numeric) AS tyb_win_odds,                                                                                                                                 
     COALESCE(t.tyb_place_odds, (0)::numeric) AS tyb_place_odds,                                                                                                                             
     COALESCE(t.tyb_equipment_change, (0)::numeric) AS tyb_equipment_change,                                                                                                                 
     COALESCE(t.tyb_leg_info, (0)::numeric) AS tyb_leg_info,                                                                                                                                 
     COALESCE(t.tyb_body_code, (0)::numeric) AS tyb_body_code,                                                                                                                               
     COALESCE(v53.tt_bias_score, (0)::double precision) AS h1_bias_score,                                                                                                                    
     COALESCE(v53.tt_alpha_signal, (0)::double precision) AS h1_alpha_signal,                                                                                                                
     COALESCE(v53.tt_confidence, (0)::double precision) AS h1_confidence,                                                                                                                    
     COALESCE(v53.tt_video_count, (0)::bigint) AS h1_video_count,                                                                                                                            
     (COALESCE(vg.fu2, 0))::double precision AS feat_vg_fu2,                                                                                                                                 
     (COALESCE(vg."優先値", 0))::double precision AS feat_vg_yusen,                                                                                                                          
     (COALESCE(vg."Ｆ指数", 0))::double precision AS feat_vg_f_index,                                                                                                                        
     (COALESCE(v53.tt_alpha_signal, (0)::double precision) * (COALESCE(v53.idm_zscore, (0)::numeric))::double precision) AS h1_idm_cross,                                                    
         CASE                                                                                                                                                                                
             WHEN (v53.surface_type = 'Turf'::text) THEN                                                                                                                                     
             CASE                                                                                                                                                                            
                 WHEN ((v53.dist_category = 1) AND (COALESCE(v53.tyb_track_cond, (1)::numeric) <= (1)::numeric)) THEN 'Core_Sprint_Turf_Good'::text                                          
                 WHEN ((v53.dist_category = 1) AND (COALESCE(v53.tyb_track_cond, (1)::numeric) > (1)::numeric)) THEN 'Core_Sprint_Turf_Soft'::text                                           
                 WHEN ((v53.dist_category = 2) AND (COALESCE(v53.tyb_track_cond, (1)::numeric) <= (1)::numeric)) THEN 'Core_Mile_Turf_Good'::text                                            
                 WHEN ((v53.dist_category = 2) AND (COALESCE(v53.tyb_track_cond, (1)::numeric) > (1)::numeric)) THEN 'Core_Mile_Turf_Soft'::text                                             
                 WHEN ((v53.dist_category >= 3) AND (COALESCE(v53.tyb_track_cond, (1)::numeric) <= (1)::numeric)) THEN 'Core_Route_Turf_Good'::text                                          
                 WHEN ((v53.dist_category >= 3) AND (COALESCE(v53.tyb_track_cond, (1)::numeric) > (1)::numeric)) THEN 'Core_Route_Turf_Soft'::text                                           
                 ELSE 'Chaos_Turf'::text                                                                                                                                                     
             END                                                                                                                                                                             
             WHEN (v53.surface_type = 'Dirt'::text) THEN                                                                                                                                     
             CASE                                                                                                                                                                            
                 WHEN ((v53.dist_category = 1) AND (COALESCE(v53.tyb_track_cond, (1)::numeric) <= (1)::numeric)) THEN 'Core_Sprint_Dirt_Good'::text                                          
                 WHEN ((v53.dist_category = 1) AND (COALESCE(v53.tyb_track_cond, (1)::numeric) > (1)::numeric)) THEN 'Core_Sprint_Dirt_Soft'::text                                           
                 WHEN ((v53.dist_category = 2) AND (COALESCE(v53.tyb_track_cond, (1)::numeric) <= (1)::numeric)) THEN 'Core_Mile_Dirt_Good'::text                                            
                 WHEN ((v53.dist_category = 2) AND (COALESCE(v53.tyb_track_cond, (1)::numeric) > (1)::numeric)) THEN 'Core_Mile_Dirt_Soft'::text                                             
                 WHEN ((v53.dist_category >= 3) AND (COALESCE(v53.tyb_track_cond, (1)::numeric) <= (1)::numeric)) THEN 'Core_Route_Dirt_Good'::text                                          
                 WHEN ((v53.dist_category >= 3) AND (COALESCE(v53.tyb_track_cond, (1)::numeric) > (1)::numeric)) THEN 'Core_Route_Dirt_Soft'::text                                           
                 ELSE 'Chaos_Dirt'::text                                                                                                                                                     
             END                                                                                                                                                                             
             WHEN (v53.surface_type = 'Steeple'::text) THEN 'Hurdle'::text                                                                                                                   
             ELSE 'Chaos_Unknown'::text                                                                                                                                                      
         END AS moe_regime                                                                                                                                                                   
FROM {{ ref('v_inference_today_v53') }} v53                                                                                                                                                   
LEFT JOIN tyb_agg t ON (((t.race_date = v53.race_date) AND (t.race_key = ((v53.venue_code || v53.race_date) || lpad(v53.race_no::text, 2, '0'::text))) AND ((t.umaban)::text = v53.umaban::text)))
LEFT JOIN vg_data vg ON (((vg.race_date = v53.race_date) AND (vg.race_no = (v53.race_no)::integer) AND (vg.umaban = (v53.umaban)::integer)))
