# A3 Sovereign Architecture - 10. H1 Geocoding

> [!NOTE]
> **Phase 439 Update Notice (2026-05-13)**
> This document was originally established in Phase 306. As of Phase 439, the A3 Sovereign Pipeline has fully transitioned to the **"Fat DB / Thin Python"** architecture.
> - **Data Ingestion**: JVD `jvd_ra` parsing offset drifts are permanently resolved via bytes-based strict extraction. `jvd_hc` is ingested via `SLOP` dataspec.
> - **Governance**: Fully automated declarative testing using `elementary-data`, `soda-core`, and `sqlfluff` within the dbt DAG. Legacy Python-based audit stubs are deprecated.
> - **Inference Topology**: Residual Pandas-based aggregations have been ported into DB-native dbt models.
> - **Execution**: Bucket Relay queue maintains zero-drift health.
> Please refer to the current `HANDOVER_NOTE.md` and Phase 439 metadata for the absolute source of operational truth.


## 1. 概要
H1で抽出された歴史イベントに位置情報を付与し、地図上で可視化するパイプライン。

## 2. 地理的マッピング
- 有効座標を持つイベント: 209件
- GeoJSON: `chrono_madness_map_v3.geojson`
- 可視化UI: `kepler_map.html`

## 3. 場所別年表
- dbtモデル: `mart_h1_timeline_location` (場所単位)
- dbtモデル: `mart_h1_timeline_person` (人物単位)
- key_figuresのUnnest（行展開）による多次元分析

*Phase 420 / 209件有効座標 / dbt年表モデル稼働中*
