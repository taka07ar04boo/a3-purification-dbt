# A3 Sovereign Architecture - 07. 自律統治とオーケストレーション (Governance & Audit)

> [!NOTE]
> **Phase 439 Update Notice (2026-05-13)**
> This document was originally established in Phase 306. As of Phase 439, the A3 Sovereign Pipeline has fully transitioned to the **"Fat DB / Thin Python"** architecture.
> - **Data Ingestion**: JVD `jvd_ra` parsing offset drifts are permanently resolved via bytes-based strict extraction. `jvd_hc` is ingested via `SLOP` dataspec.
> - **Governance**: Fully automated declarative testing using `elementary-data`, `soda-core`, and `sqlfluff` within the dbt DAG. Legacy Python-based audit stubs are deprecated.
> - **Inference Topology**: Residual Pandas-based aggregations have been ported into DB-native dbt models.
> - **Execution**: Bucket Relay queue maintains zero-drift health.
> Please refer to the current `HANDOVER_NOTE.md` and Phase 439 metadata for the absolute source of operational truth.


## 1. 概要
A3が強固なエンタープライズシステムとして長期間運用されるための絶対的な統治機構。エラーの自己診断からパージ、構造的劣化の監視までを自律実行。Phase 400以降、ガバナンスの中心をPythonからdbt testへ大幅に移行。

## 2. 統合ガバナンスエンジン

### 2.1 `a3_governance.py --preflight` (Table-Driven)
全AIセッション開始時に必須実行。DB駆動型のチェック体制。
- **A: データパイプライン:** 本日のJRDB/SED取込み状況
- **B: 特徴量・推論:**
  - B-06: TYB直前特徴量 ≥ 5カラム (DGRD-13)
  - B-07: 学習matview存在 (DGRD-14)
  - B-09: 特徴量総数 ≥ 70カラム
- **F: バケツリレー健全性:** ZOMBIE/FAILED/QUEUED滞留の監視
- **DGRD-08:** Pythonファイル数 < 300

### 2.2 `governance_kickstart()` DB関数 (Phase 402導入)
1 SQLで以下を自動実行:
- メタ学習記録
- BR GOVキュー投入 (Queue Health, FAILED Alert, Design Parity, Session Context)
- セッション開始の公式記録

### 2.3 dbt test (56テスト — Phase 400-420で大幅拡充)
Pythonガバナンスからの段階的移行先。全テスト自動実行。

| カテゴリ | テスト数 | 内容 |
|---|---|---|
| データ品質 | 15+ | NULL率監視、正規表現、JVDデータ存在 |
| ガバナンス | 10+ | カラム数下限、TakeTube特徴量、sire/bms非ゼロ率 |
| Chaos検知 | 3 | is_chaos存在、chaos比率0-20%、閾値DB同期 |
| BR健全性 | 3 | FAILED<50、RECURRING≥50、孤立依存検出 |
| 回帰テスト | 5+ | regime多様性、view chainカラム数、データ鮮度 |
| 推論活性 | 2 | inference_freshness、BR recurring_activity |
| その他 | 10+ | meta_learning蓄積、JVDオフセットずれ再発防止等 |

## 3. 多層防御レイヤー

| レイヤー | ツール | 検出対象 | 状態 |
|---|---|---|---|
| dbt test | 56テスト全自動 | データ品質+ガバナンス | ✅ 主力 |
| DB関数 | governance_kickstart() | セッション開始自動化 | ✅ |
| 契約 | a3_contracts.py | 特徴量・型の欠落 | ✅ |
| 品質 | a3_evidently_gate.py | データドリフト | ✅ BR自動 |
| Chaos SSOT | a3_meta.chaos_thresholds | 閾値一元管理 | ✅ Phase 415 |
| 設計 | api.a3_design_registry | 設計書陳腐化 | ✅ |
| リアルタイム | DBトリガー3種 | Phase自動バンプ等 | ✅ |
| メタ学習 | system_meta_learning_history | 再発パターン警告 (194件) | ✅ |
| ソース鮮度 | dbt source freshness | kyi_parsed/sed_parsed鮮度 | ✅ 2/2 PASS |

## 4. Automated Handover (自動引継ぎ)
- **`HANDOVER_NOTE.md`:** Known Failures、Spec Gaps、次回最優先タスクを表形式で保存
- パッチ vs 根本修正の厳密な区別記録
- **セッション開始時の義務:**
  1. governance_kickstart(PhaseN, 'AgentName') 実行
  2. 前セッションの設計書・メタ学習更新チェック
  3. BRキュー健全性確認
  4. BR投入可能なタスクの洗い出し

## 5. メタ学習システム (Phase 420: 194件)
`api.system_meta_learning_history` に全教訓を蓄積。
- CRITICAL_BUG, GOVERNANCE, ARCHITECTURE等のカテゴリ分類
- severity: LOW/MEDIUM/HIGH/CRITICAL
- KI(Knowledge Item)への昇格プロセス

*Phase 420 / dbt test 56件 / ML 194件 / governance_kickstart() 稼働中*
