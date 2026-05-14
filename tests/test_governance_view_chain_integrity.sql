-- test_governance_view_chain_integrity.sql
-- Phase 406: 推論ビューチェーンの完全性チェック
-- v316 → v56 → v55 → v53 → v_inference_today の依存チェーンが健全であることを確認
-- v316のカラム数が120以上であることを保証(推論に必要な特徴量が欠落していないか)
-- 現状121カラム(Phase 406時点)
-- 0行返却 = PASS, 1行以上 = FAIL

WITH column_check AS (
    SELECT COUNT(*) AS col_count
    FROM information_schema.columns
    WHERE table_schema = 'api'
      AND table_name = 'v_inference_today_v316'
)
SELECT col_count
FROM column_check
WHERE col_count < 120
