# A3 Sovereign Architecture - 08. H1 ChronoMap Pipeline (歴史コンテキスト)

> [!NOTE]
> **Phase 439 Update Notice (2026-05-13)**
> This document was originally established in Phase 306. As of Phase 439, the A3 Sovereign Pipeline has fully transitioned to the **"Fat DB / Thin Python"** architecture.
> - **Data Ingestion**: JVD `jvd_ra` parsing offset drifts are permanently resolved via bytes-based strict extraction. `jvd_hc` is ingested via `SLOP` dataspec.
> - **Governance**: Fully automated declarative testing using `elementary-data`, `soda-core`, and `sqlfluff` within the dbt DAG. Legacy Python-based audit stubs are deprecated.
> - **Inference Topology**: Residual Pandas-based aggregations have been ported into DB-native dbt models.
> - **Execution**: Bucket Relay queue maintains zero-drift health.
> Please refer to the current `HANDOVER_NOTE.md` and Phase 439 metadata for the absolute source of operational truth.


## 1. 概要
競馬の歴史的事件・人物・場所の知識を構造化し、推論に反映するプロジェクト。A3本体とは分離管理（models/h1_historyディレクトリ）。

## 2. パイプライン
```
api.a3_h1_raw_chunks (17,915件, embedding済み 100% ✅)
    ↓ a3_h1_embed_chunks.py
chrono_archive.extracted_events (134,581行)
    ↓ dbt models
mart_h1_timeline_person (人物別年表)
mart_h1_timeline_location (場所別年表)
v_h1_pattern_summary (パターン分析)
v_h1_race_context (レースコンテキスト)
v_h1_embedding_progress (進捗)
```

## 3. 抽出と品質
- 抽出済み: ~3,000件で停止中 (F-23: LLM APIレートリミット)
- 有効座標を持つイベント: 209件
- GeoJSON出力: `chrono_madness_map_v3.geojson`
- UI: `kepler_map.html`

## 4. ベクトルDB (Qdrant)
- a3-qdrant (port 6333)
- 17,915チャンク全てembedding済み (100%)

*Phase 420 / embedding 100% / dbt models統合*
