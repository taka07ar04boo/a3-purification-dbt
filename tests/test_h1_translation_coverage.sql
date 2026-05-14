-- test_h1_translation_coverage.sql
-- H1 データ品質ガバナンス: 抽出されたイベントの翻訳(Modern, Layman, AI)カバレッジが100%であること
-- 0行返却 = PASS, 1行以上 = FAIL

WITH missing_translations AS (
    SELECT id
    FROM chrono_archive.extracted_events
    WHERE event_description_modern IS NULL
       OR event_description_modern = ''
       OR event_description_layman IS NULL
       OR event_description_layman = ''
       OR ai_interpretation IS NULL
       OR ai_interpretation = ''
)
SELECT * FROM missing_translations
