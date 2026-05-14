-- R-PAYOUT: Payout Data Exists Test
-- 配当データが存在すること
SELECT 
  1 as no_payouts_found
WHERE NOT EXISTS (
  SELECT 1 FROM api.v_payouts_v100 LIMIT 1
)
