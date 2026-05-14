# A3 Sovereign Architecture - 16. システム監査と健全性監視 (System Audit & Health Monitoring)

> [!NOTE]
> **Phase 439 Update Notice (2026-05-13)**
> This document was originally established in Phase 306. As of Phase 439, the A3 Sovereign Pipeline has fully transitioned to the **"Fat DB / Thin Python"** architecture.
> - **Data Ingestion**: JVD `jvd_ra` parsing offset drifts are permanently resolved via bytes-based strict extraction. `jvd_hc` is ingested via `SLOP` dataspec.
> - **Governance**: Fully automated declarative testing using `elementary-data`, `soda-core`, and `sqlfluff` within the dbt DAG. Legacy Python-based audit stubs are deprecated.
> - **Inference Topology**: Residual Pandas-based aggregations have been ported into DB-native dbt models.
> - **Execution**: Bucket Relay queue maintains zero-drift health.
> Please refer to the current `HANDOVER_NOTE.md` and Phase 439 metadata for the absolute source of operational truth.


## 1. 概要
A3の健全性は多層的な自動監視メカニズムで保証されています。Phase 400以降、監視の中心がPythonスクリプトからdbt testへ大幅に移行し、自動化率が飛躍的に向上しました。

## 2. 監視レイヤー

### 2.1 dbt test (56テスト — 主力)
| テスト名 | 内容 |
|---|---|
| test_governance_chaos_detection | is_chaos=True ≥ 1件 (再発防止) |
| test_governance_chaos_ratio | Chaos比率 0〜20% |
| test_governance_chaos_threshold_db_sync | chaos_thresholds SSOT整合性 |
| test_governance_regime_distribution | regime多様性 ≥ 4 |
| test_governance_view_chain_integrity | v316カラム数 ≥ 120 |
| test_governance_column_count | features base ≥ 220カラム |
| test_governance_taketube_columns | feat_tt_* ≥ 5カラム |
| test_governance_sire_bms_nonzero | sire/bms非ゼロ率 ≥ 80% |
| test_governance_data_freshness | kyi 14日以内 |
| test_governance_br_failed | BR FAILED < 50 |
| test_governance_br_recurring | RECURRING ≥ 50 |
| test_governance_br_orphan_dependency | FAILED孤立依存検出 |
| test_governance_inference_freshness | 推論ログ7日以内に存在 |
| test_governance_br_recurring_activity | RECURRING完了 24h ≥ 50件 |
| test_governance_python_file_count | Pythonファイル < 300 |
| test_jvd_data_coverage | JVDデータ存在 |
| test_jvd_ra_data_quality | JVDオフセットずれ再発防止 |
| test_meta_learning_count | ML ≥ 100件 |
| その他 | H1品質、NULL率、正規表現等 |

### 2.2 dbt source freshness
- kyi_parsed: warn 7日 / error 14日
- sed_parsed: warn 7日 / error 14日
- **2/2 PASS**

### 2.3 `a3_governance.py --preflight`
- Table-Driven型 (DB駆動)
- CRITICAL/WARNING/INFO レベル分類
- `--fix` で自動修復モード

### 2.4 BR RECURRING 自動監視 (118件)
| 項目 | 間隔 | 内容 |
|---|---|---|
| dbt Test Runner | 5サイクル | dbt test全実行 |
| Evidently Quality Gate | 5サイクル | データドリフト検出 |
| Spec Drift Monitor | 5サイクル | 設計書-実DB突合 |
| KI Freshness Scanner | 5サイクル | KI陳腐化検出 |
| Zombie Reaper | Pre-flight | IN_PROGRESS 30min超→QUEUED |
| Session Context Snapshot | 1h | 構造化JSONBパッケージ |
| Design Freshness Guard | 5サイクル | design_registry staleness |
| DGRD-08 Smart Archiver | RECURRING | Pythonファイル数監視 |
| PostRace Auto-Check | RECURRING | レース後自動チェック |

## 3. 監査ツール稼働状況

| ツール | 状態 | 用途 |
|---|---|---|
| **dbt test** | ✅ 56/56 PASS | データ整合性+ガバナンス |
| **Evidently 0.7.21** | ✅ BR自動 | データドリフト検出 |
| **a3_governance.py** | ✅ preflight | ガバナンスチェック |
| **pytest 9.0.3** | ✅ 利用可能 | ユニットテスト |
| **sqlfluff** | ❌ 未導入 | SQL品質リント |
| **elementary-data** | ❌ 未導入 | dbt監視ダッシュボード |

*Phase 420 / dbt test 56件 / source freshness 2/2 / RECURRING 118件*
