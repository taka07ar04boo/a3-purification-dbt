-- R-SOVEREIGN: Sovereign Rules Active Test  
-- 全てのCRITICALルールがアクティブであること
SELECT 
  rule_code,
  rule_name
FROM api.a3_sovereign_rules
WHERE severity = 'CRITICAL' AND is_active = false
