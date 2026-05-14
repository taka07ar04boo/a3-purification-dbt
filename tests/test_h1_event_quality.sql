-- test_h1_event_quality.sql
-- H1 データ品質ガバナンス: 抽出されたイベントの品質チェック
-- 必須フィールド (category, description) のNULL率が95%以上であること
-- location のNULL率が90%以上であること
-- 0行返却 = PASS, 1行以上 = FAIL

WITH quality AS (
    SELECT
        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE event_category IS NOT NULL AND event_category != '') AS has_category,
        COUNT(*) FILTER (WHERE event_description IS NOT NULL AND event_description != '') AS has_description,
        COUNT(*) FILTER (WHERE event_location IS NOT NULL AND event_location != '') AS has_location
    FROM chrono_archive.extracted_events
)
SELECT
    total,
    has_category * 100 / GREATEST(total, 1) AS category_pct,
    has_description * 100 / GREATEST(total, 1) AS description_pct,
    has_location * 100 / GREATEST(total, 1) AS location_pct
FROM quality
WHERE has_category * 100 / GREATEST(total, 1) < 95
   OR has_description * 100 / GREATEST(total, 1) < 95
   OR has_location * 100 / GREATEST(total, 1) < 90
