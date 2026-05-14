-- R-H1: H1 Data Pipeline Integrity
-- H1 ETLが正常にデータを転送していることを確認
SELECT 
  'h1_events_empty' as failure_reason
WHERE NOT EXISTS (SELECT 1 FROM api.a3_h1_events LIMIT 1)
UNION ALL
SELECT 
  'h1_raw_chunks_empty' as failure_reason
WHERE NOT EXISTS (SELECT 1 FROM api.a3_h1_raw_chunks LIMIT 1)
UNION ALL
SELECT 
  'h1_context_features_empty' as failure_reason
WHERE NOT EXISTS (SELECT 1 FROM api.a3_h1_context_features LIMIT 1)
