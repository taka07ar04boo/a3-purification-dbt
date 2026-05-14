# A3 Sovereign Architecture - 11. バケツリレー詳細設計 (Bucket Relay Orchestration)

> [!NOTE]
> **Phase 439 Update Notice (2026-05-13)**
> This document was originally established in Phase 306. As of Phase 439, the A3 Sovereign Pipeline has fully transitioned to the **"Fat DB / Thin Python"** architecture.
> - **Data Ingestion**: JVD `jvd_ra` parsing offset drifts are permanently resolved via bytes-based strict extraction. `jvd_hc` is ingested via `SLOP` dataspec.
> - **Governance**: Fully automated declarative testing using `elementary-data`, `soda-core`, and `sqlfluff` within the dbt DAG. Legacy Python-based audit stubs are deprecated.
> - **Inference Topology**: Residual Pandas-based aggregations have been ported into DB-native dbt models.
> - **Execution**: Bucket Relay queue maintains zero-drift health.
> Please refer to the current `HANDOVER_NOTE.md` and Phase 439 metadata for the absolute source of operational truth.


## 1. 概要
状態主導型の自律オーケストレーター。636,663タスク完了、118件のRECURRINGタスクが常時自動実行。

## 2. 実行トポロジー
```
a3_worker_entrypoint.sh
  ├─ [bg] a3_health_scheduler.py → 定期的にa3_health_check.py実行
  ├─ [bg] a3_jvlink_ingest.py → JRA-VANデータの自動取込み
  └─ [PID 1] a3_pipeline_orchestrator.py
       └─ PipelineOrchestrator.run_daemon()
            ├─ _br_preflight() → 非正規type正規化(7パターン) + ZOMBIE Reaper(30分超)
            ├─ [5サイクル毎] auto_auditor_daemon.run_all_audits()
            └─ [毎サイクル] _process_independent_br_tasks(batch=5)
```

## 3. 16種のディスパッチタイプ
| Type | 概要 | 例 |
|---|---|---|
| SQL_EXEC | 生SQL実行 | ガバナンスチェック、DDL |
| EXECUTE | Python実行 | tmpfile → subprocess |
| DOCKER_EXEC | コンテナコマンド | dbt test, データ取込 |
| FILE_EDIT | ファイル置換 | JSON形式の修正指示 |
| LLM_PROMPT | LLM傭兵 | 7段フォールバック自動コード生成 |
| DECOMPOSER | タスク分解 | 62パターンのインターセプト |
| AUDIT | 監査実行 | 品質チェック |
| CLEANUP | 削除処理 | DROP/DELETE/RM |
| BROWSER | Playwright操作 | スクレイピング |
| MATVIEW_REFRESH | MV更新 | pre/postカウント検証 |
| DB_MIGRATE | DDL実行 | SAVEPOINT安全 |
| その他 | TEST_RUNNER, GIT_COMMIT, ARCHIVE, HEALTH_CHECK, HUMAN | |

## 4. 傭兵LLM 7段フォールバック
`LLM_PROMPT` タイプで軽微なコード修正を自動処理:
Gemini → Groq → OpenRouter → Cerebras → GitHub → HuggingFace → Ollama

## 5. DLQ (Dead Letter Queue) ライフサイクル
```
FAILED (retry_count >= max_retries)
  → br_dead_letter_queue (status=PENDING)
    → P45 Auto-Medic (分類: FILE_MISSING/TYPE_MISMATCH/TIMEOUT/SCHEMA_MISMATCH/UNKNOWN)
      → L1リトライ or L2ヒーリング(LLM_PROMPT) → RESOLVED/ESCALATED
```

## 6. 統計 (Phase 420)
| 項目 | 値 |
|---|---|
| COMPLETED | **636,663** |
| QUEUED | 106 |
| FAILED (24h) | **0** ✅ |
| IN_PROGRESS | 2 (RECURRING正常) |
| CANCELLED | 11 |
| ARCHIVED | 808 |
| RECURRING登録 | 118件 |
| Decomposerパターン | 62パターン |

*Phase 420 / FAILED(24h) 0 / 636K+ completed*
