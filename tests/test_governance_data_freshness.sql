-- test_governance_data_freshness.sql
-- レースデータの鮮度を確認
-- kyi_parsed (前日データ) に最新7日以内のデータが存在することを確認
-- 非レースデーにも対応: 直近のingested_atが7日以上前ならWARNレベル

-- このテストは、最新のingested_atが14日以上古い場合にFAILする
-- (source freshnessと同じ閾値だが、dbt testとしても独立して実行可能)
SELECT 1
WHERE (
    SELECT EXTRACT(EPOCH FROM (NOW() - MAX(ingested_at))) / 86400
    FROM public.kyi_parsed
) > 14
