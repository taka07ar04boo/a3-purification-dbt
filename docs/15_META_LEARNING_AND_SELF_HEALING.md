# A3 Sovereign Architecture - 15. メタ学習と自己修復 (Meta-Learning & Self-Healing)

> [!NOTE]
> **Phase 439 Update Notice (2026-05-13)**
> This document was originally established in Phase 306. As of Phase 439, the A3 Sovereign Pipeline has fully transitioned to the **"Fat DB / Thin Python"** architecture.
> - **Data Ingestion**: JVD `jvd_ra` parsing offset drifts are permanently resolved via bytes-based strict extraction. `jvd_hc` is ingested via `SLOP` dataspec.
> - **Governance**: Fully automated declarative testing using `elementary-data`, `soda-core`, and `sqlfluff` within the dbt DAG. Legacy Python-based audit stubs are deprecated.
> - **Inference Topology**: Residual Pandas-based aggregations have been ported into DB-native dbt models.
> - **Execution**: Bucket Relay queue maintains zero-drift health.
> Please refer to the current `HANDOVER_NOTE.md` and Phase 439 metadata for the absolute source of operational truth.


## 1. 概要
A3は過去の障害・教訓を構造化して蓄積し、同じ問題の再発を防止するメタ学習システムを備えています。Phase 420時点で194件の教訓が蓄積され、うちCRITICALレベルの6件がKI(Knowledge Item)に昇格済み。

## 2. メタ学習データベース (`api.system_meta_learning_history`)

### 2.1 スキーマ
| カラム | 型 | 説明 |
|---|---|---|
| id | SERIAL | PK (Max: 935) |
| session_phase | INT | 発生Phase |
| category | VARCHAR | BUG_FIX / GOVERNANCE / ARCHITECTURE / CRITICAL_BUG / PERFORMANCE等 |
| summary | TEXT | 概要 |
| root_cause | TEXT | 根本原因 |
| resolution | TEXT | 解決策 |
| prevention_mechanism | TEXT | 再発防止策 |
| severity | VARCHAR | LOW / MEDIUM / HIGH / CRITICAL |

### 2.2 蓄積状況 (Phase 420)
- **合計: 194件**
- governance_kickstart()による自動記録 + 手動記録
- Phase 420新規: dbt run ERROR 3件修正 (JVD固定長パース不正データ)

## 3. KI (Knowledge Item) システム
メタ学習の重要教訓を永続化したナレッジベース。

### 3.1 主要KI一覧
| KI | 内容 | 参照タイミング |
|---|---|---|
| A3_AUTOMATED_HANDOVER | セッション引継ぎ書 | 毎セッション |
| A3_IMMUTABLE_CORE_RULES | コア憲法13条 | 毎セッション |
| A3_BUCKET_RELAY_ARCHITECTURE | BR詳細設計 | BR問題時 |
| A3_ARCHITECTURE_DEGRADE_VACCINE | AIドリフト防止等6ワクチン | 毎セッション |
| A3_POWERSHELL_DOCKER_VACCINE | PS→Docker実行パターン | Docker操作時 |
| A3_RACEDAY_PIPELINE | レースデー手順 | レース前 |
| A3_VG_DATA_PROTECTION | VictGrabデータ保護 | VGカラム操作時 |
| A3_SECRETS_ARCHITECTURE | 環境変数・APIキー管理 | 毎セッション |

### 3.2 KIギャップ
- CRITICAL ML 15件中KI化 ~6件 → 昇格プロセスの自動化が必要

## 4. Chaos閾値一元管理 (Phase 415 SSOT)
`a3_meta.chaos_thresholds` テーブルでChaos検知閾値を一元管理。
- Phase 405: v316.sqlのChaos検知が100%無効化(is_chaos常にfalse)していた問題を修正
- Phase 414: a3_router.pyへの同期修正漏れを発見・修正
- Phase 415: さらに4ファイルに旧閾値残存を発見・修正 + DB SSOT作成

## 5. 自己修復メカニズム
- **BR DLQ Auto-Medic:** FAILEDタスクの自動分類→リトライ→エスカレーション
- **ZOMBIE Reaper:** IN_PROGRESS 30分超のタスクを自動QUEUED戻し
- **governance_kickstart():** セッション開始時のBRキュー自動投入
- **session_context_package:** 1h間隔で構造化JSONBパッケージを自動更新

*Phase 420 / ML 194件 / KI 12+ / Chaos SSOT稼働中*
