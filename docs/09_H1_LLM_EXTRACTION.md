# A3 Sovereign Architecture - 09. H1 LLM Extraction

> [!NOTE]
> **Phase 439 Update Notice (2026-05-13)**
> This document was originally established in Phase 306. As of Phase 439, the A3 Sovereign Pipeline has fully transitioned to the **"Fat DB / Thin Python"** architecture.
> - **Data Ingestion**: JVD `jvd_ra` parsing offset drifts are permanently resolved via bytes-based strict extraction. `jvd_hc` is ingested via `SLOP` dataspec.
> - **Governance**: Fully automated declarative testing using `elementary-data`, `soda-core`, and `sqlfluff` within the dbt DAG. Legacy Python-based audit stubs are deprecated.
> - **Inference Topology**: Residual Pandas-based aggregations have been ported into DB-native dbt models.
> - **Execution**: Bucket Relay queue maintains zero-drift health.
> Please refer to the current `HANDOVER_NOTE.md` and Phase 439 metadata for the absolute source of operational truth.


## 1. 概要
H1 ChronoMapの歴史テキストからLLMで構造化イベントを抽出するパイプライン。

## 2. 抽出エンジン (`a3_h1_llm_extractor.py`)
- LLM API (Groq/DeepSeek等) でテキストチャンクからイベント抽出
- 出力: category, description, location, key_figures, date等

## 3. 現状 (Phase 420)
- 抽出済み: ~3,000件で停止中 (F-23)
- 原因: LLM APIレートリミット(429)でRATE_LIMITED状態
- 対策: BR RECURRINGによる自動分割処理を計画

## 4. 品質チェック (dbt test)
- `test_h1_extraction_coverage`: イベント≥500件
- `test_h1_event_quality`: category/description 95%+, location 90%+
- BR RECURRING: `[P401-AUTO] H1 Quality Monitor` 週次自動実行

*Phase 420 / 抽出~3,000件 / レートリミット対策計画中*
