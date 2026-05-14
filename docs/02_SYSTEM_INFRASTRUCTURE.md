# A3 Sovereign Architecture - 02. インフラストラクチャと実行環境 (Infrastructure)

> [!NOTE]
> **Phase 439 Update Notice (2026-05-13)**
> This document was originally established in Phase 306. As of Phase 439, the A3 Sovereign Pipeline has fully transitioned to the **"Fat DB / Thin Python"** architecture.
> - **Data Ingestion**: JVD `jvd_ra` parsing offset drifts are permanently resolved via bytes-based strict extraction. `jvd_hc` is ingested via `SLOP` dataspec.
> - **Governance**: Fully automated declarative testing using `elementary-data`, `soda-core`, and `sqlfluff` within the dbt DAG. Legacy Python-based audit stubs are deprecated.
> - **Inference Topology**: Residual Pandas-based aggregations have been ported into DB-native dbt models.
> - **Execution**: Bucket Relay queue maintains zero-drift health.
> Please refer to the current `HANDOVER_NOTE.md` and Phase 439 metadata for the absolute source of operational truth.


## 1. 概要
A3 Reactorはコンテナ化されたハイブリッドアーキテクチャを採用。Docker（Linux）上のAIパイプラインと、WindowsホストOS上のJRA-VAN連携が、PostgreSQLを介して自律的に協調動作します。

## 2. Docker コンテナアーキテクチャ (7コンテナ)

### 2.1 PostgreSQL (`a3-postgres-15`)
- **PG15 + pgvector:** 全データ、メタデータ、ベクトル埋め込みを一元管理
- **スキーマ:** `public`(JRDBデータ/JVDテーブル), `api`(A3固有機能), `a3_meta`(BR/ガバナンス), `chrono_archive`(H1)
- **DB-First (Fat DB / Thin Python):** 推論チェーン全体がdbt DAG管理下。設定やメタデータすらもテーブルに格納
- **JVDテーブル:** 16テーブル中13テーブル存在 (jvd_se: 2,853,079件, jvd_um: 198,212件, jvd_ks: 1,493件, jvd_ch: 1,431件, jvd_ra/hr: 各72件, jvd_o1~o6)

### 2.2 Sovereign Worker (`a3-sovereign-worker`)
- **PID 1:** `a3_pipeline_orchestrator.py` がメインプロセスとして常駐
- **依存:** CatBoost, LightGBM, PyTorch, TabNet, Playwright, sentence-transformers, dbt-core 1.11.9等
- **Python 3.11:** 198個のスクリプト（上限300, DGRD-08）

### 2.3 補助コンテナ群
- **a3-bootloader:** 5分間隔の初期化監視、Gemini APIで自律コード生成
- **a3-qdrant:** ベクトルDB（H1 embedding検索用, 17,915/17,915 = 100%完了）
- **a3-mlflow:** 実験管理（port 5000）
- **a3-ollama:** ローカルLLM実行環境
- **a3-hatchet:** ワークフローエンジン（Restarting既知、非クリティカル）

## 3. Windows Host — JV-Link COM連携
JRA-VANの「JV-Link」はWindowsネイティブのCOMコンポーネント(32bit)。

### 3.1 `a3_jravan_auto_ingester.py` (Phase 389実装, Phase 419突破)
- **32bit Python 3.11** + `win32com.client` でJVDTLab.JVLink COMを直接操作
- dataspec×option組合せ: RACE(opt1-4✅), DIFF(opt3/4のみ✅), TOKU(opt1/2✅), MING/WOOD/SLOP(opt1/3/4✅)
- Phase 419で12セッション連続ブロッカー解消: `DIFF option=3`でUM/KS/CH 201,136件取込

## 4. dbt (Data Build Tool) アーキテクチャ
- **dbt-core 1.11.9 + dbt-postgres 1.9.0**
- **81モデル全PASS** (staging→intermediate→mart→inference→reporting)
- **56テスト全PASS** (データ品質/ガバナンス/回帰テスト)
- **推論チェーン全体がdbt管理下:** v_inference_today→v53→v55→v56→v316
- **未dbt化VIEW: 0件** (Phase 411で達成)

## 5. Bucket Relay Orchestrator
Cronではなく状態主導型の自律オーケストレーター。
- タスクキュー: `a3_meta.a3_sub_tasks` (19カラム, 16種の実行タイプ)
- 依存関係のDAG解決、ZOMBIE検出、DLQ(Dead Letter Queue)による自己修復
- 118件のRECURRINGタスクが自動実行
- **636,663タスク完了** (FAILED 0, QUEUED 106)
- 傭兵LLM 7段フォールバック: Gemini→Groq→OpenRouter→Cerebras→GitHub→HuggingFace→Ollama

## 6. システム統計 (Phase 420時点)

| 項目 | 値 |
|---|---|
| Python Files | 198個 (上限300) |
| dbt Models | **81/81 PASS** |
| dbt Tests | **56/56 PASS** |
| dbt Source Freshness | 2/2 PASS |
| BR COMPLETED | **636,663** |
| BR RECURRING | 118件 |
| RECURRING Tasks (24h) | ~76,000件/日 |
| Feature Coverage | **259 Columns** (a3_features_today_base) |
| JVD Tables | 13/16 存在 |
| Meta Learning | **194件** |
| DB Connection | PG15 (host='db', port=5432, dbname=pckeiba) |
| DB Backup | pckeiba_20260512.dump (915MB, 5世代保持) |
| Chaos閾値SSOT | a3_meta.chaos_thresholds (DB一元管理) |
| governance_kickstart() | DB関数稼働中 |
| session_context_package | 1h間隔RECURRING |

*Phase 420 / 2026-05-13 更新*
