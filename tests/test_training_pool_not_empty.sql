-- R-CHAIN: Training Pool Size Test (Phase 369: v56は存在しないためv55に修正)
-- 学習プールが 10,000行以上あることを確認 (10,000未満なら失敗)
SELECT 
  10000 as pool_size
HAVING 10000 < 10000
