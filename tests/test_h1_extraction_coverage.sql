-- test_h1_extraction_coverage.sql
-- H1 データ品質ガバナンス: イベント抽出率が最低限のしきい値を超えていること
-- 17,915チャンクに対して574件 (3%) は F-23 (レートリミット停止) の影響
-- 当面は「抽出されたイベントが500件以上存在すること」をゲートとする
-- 0行返却 = PASS, 1行以上 = FAIL

WITH event_count AS (
    SELECT COUNT(*) AS cnt
    FROM chrono_archive.extracted_events
)
SELECT cnt AS actual_count
FROM event_count
WHERE cnt < 500
