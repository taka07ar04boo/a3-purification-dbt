{{ config(
    materialized='view',
    schema='api'
) }}

SELECT v52.race_date,                                                                                                                 
     v52.venue_code,                                                                                                                    
     v52.race_no,                                                                                                                       
     v52.umaban,                                                                                                                        
     v52.horse_id,                                                                                                                      
     v52.horse_name,                                                                                                                    
     v52.jockey_code,                                                                                                                   
     v52.trainer_code,                                                                                                                  
     v52.gate_no,                                                                                                                       
     v52.score_idm,                                                                                                                     
     v52.idx_ten,                                                                                                                       
     v52.idx_agari,                                                                                                                     
     v52.odds_win_pre,                                                                                                                  
     v52.pop_win_pre,                                                                                                                   
     v52.distance_m,                                                                                                                    
     v52.total_horses,                                                                                                                  
     v52.class_code_num,                                                                                                                
     v52.is_hurdle,                                                                                                                     
     v52.surface_type,                                                                                                                  
     v52.idm_zscore,                                                                                                                    
     v52.ten_zscore,                                                                                                                    
     v52.agari_zscore,                                                                                                                  
     v52.race_std_idm,                                                                                                                  
     v52.race_std_ten,                                                                                                                  
     v52.race_std_agari,                                                                                                                
     v52.tyb_total_score,                                                                                                               
     v52.tyb_track_cond,                                                                                                                
     v52.jvd_cushion_val,                                                                                                               
     v52.sire_system_code,                                                                                                              
     v52.bms_system_code,                                                                                                               
     v52.gekiso_index,                                                                                                                  
     v52.dist_aptitude,                                                                                                                 
     v52.stable_eval_code,                                                                                                              
     v52.training_arrow_code,                                                                                                           
     v52.gekiso_dist_cross,                                                                                                             
     v52.training_stable_cross,                                                                                                         
     v52.grass_aptitude_code,                                                                                                           
     v52.dirt_aptitude_code,                                                                                                            
     v52.running_style_code,                                                                                                            
     v52.mud_aptitude_code,                                                                                                             
     v52.running_form_code,                                                                                                             
     v52.body_type_code,                                                                                                                
     v52.rotation_days,                                                                                                                 
     v52.dist_aptitude2,                                                                                                                
     v52.longshot_index,                                                                                                                
     v52.start_index,                                                                                                                   
     v52.late_start_rate,                                                                                                               
     v52.popular_index,                                                                                                                 
     v52.jockey_exp_rentai,                                                                                                             
     v52.jockey_exp_win,                                                                                                                
     v52.jockey_exp_top3,                                                                                                               
     v52.track_fit_signal,                                                                                                              
     v52.tt_bias_score,                                                                                                                 
     v52.tt_alpha_signal,                                                                                                               
     v52.tt_confidence,                                                                                                                 
     v52.tt_video_count,                                                                                                                
     v52.low_pop_high_idm,                                                                                                              
     v52.pace_advantage_score,                                                                                                          
     v52.actual_rank,                                                                                                                   
     v52.is_winner,                                                                                                                     
     v52.is_top3,                                                                                                                       
     v52.rank_ratio,                                                                                                                    
         CASE                                                                                                                           
             WHEN ((v52.rotation_days >= (1)::numeric) AND (v52.rotation_days <= (14)::numeric)) THEN 1                                 
             WHEN ((v52.rotation_days >= (15)::numeric) AND (v52.rotation_days <= (28)::numeric)) THEN 2                                
             WHEN ((v52.rotation_days >= (29)::numeric) AND (v52.rotation_days <= (56)::numeric)) THEN 3                                
             WHEN ((v52.rotation_days >= (57)::numeric) AND (v52.rotation_days <= (90)::numeric)) THEN 4                                
             WHEN (v52.rotation_days > (90)::numeric) THEN 5                                                                            
             ELSE 0                                                                                                                     
         END AS rotation_seg,                                                                                                           
     ((COALESCE(v52.gate_no, 0))::numeric * COALESCE(v52.running_style_code, (0)::numeric)) AS gate_style_cross,                        
         CASE                                                                                                                           
             WHEN (v52.gate_no <= 3) THEN 1                                                                                             
             WHEN (v52.gate_no <= 5) THEN 0                                                                                             
             WHEN (v52.gate_no >= 6) THEN '-1'::integer                                                                                 
             ELSE 0                                                                                                                     
         END AS gate_bias_score,                                                                                                        
         CASE                                                                                                                           
             WHEN (v52.distance_m < 1400) THEN 1                                                                                        
             WHEN ((v52.distance_m >= 1400) AND (v52.distance_m <= 1800)) THEN 2                                                        
             WHEN ((v52.distance_m >= 1801) AND (v52.distance_m <= 2200)) THEN 3                                                        
             ELSE 4                                                                                                                     
         END AS dist_category,                                                                                                          
     (((COALESCE(v52.gate_no, 0))::numeric * COALESCE(v52.running_style_code, (0)::numeric)) * (                                        
         CASE                                                                                                                           
             WHEN (v52.distance_m < 1400) THEN 1                                                                                        
             WHEN ((v52.distance_m >= 1400) AND (v52.distance_m <= 1800)) THEN 2                                                        
             WHEN ((v52.distance_m >= 1801) AND (v52.distance_m <= 2200)) THEN 3                                                        
             ELSE 4                                                                                                                     
         END)::numeric) AS gate_style_dist_cross,                                                                                       
     (COALESCE(v52.running_style_code, (0)::numeric) * (                                                                                
         CASE                                                                                                                           
             WHEN ((v52.rotation_days >= (1)::numeric) AND (v52.rotation_days <= (14)::numeric)) THEN 1                                 
             WHEN ((v52.rotation_days >= (15)::numeric) AND (v52.rotation_days <= (28)::numeric)) THEN 2                                
             WHEN ((v52.rotation_days >= (29)::numeric) AND (v52.rotation_days <= (56)::numeric)) THEN 3                                
             WHEN (v52.rotation_days > (56)::numeric) THEN 4                                                                            
             ELSE 0                                                                                                                     
         END)::numeric) AS style_rotation_cross,                                                                                        
         CASE                                                                                                                           
             WHEN (((v52.rotation_days >= (1)::numeric) AND (v52.rotation_days <= (14)::numeric)) AND (v52.idm_zscore > 0.5)) THEN 1    
             ELSE 0                                                                                                                     
         END AS short_rest_high_idm,                                                                                                    
     (COALESCE(v52.tt_alpha_signal, (0)::double precision) * (COALESCE(v52.idm_zscore, (0)::numeric))::double precision) AS tt_idm_cross
    FROM {{ ref('v_inference_today') }} v52
