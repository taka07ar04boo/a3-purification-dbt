# A3 Sovereign Architecture - 01. コア哲学と絶対ルール (Core Philosophy & Rules)

> [!NOTE]
> **Phase 439 Update Notice (2026-05-13)**
> This document was originally established in Phase 306. As of Phase 439, the A3 Sovereign Pipeline has fully transitioned to the **"Fat DB / Thin Python"** architecture.
> - **Data Ingestion**: JVD `jvd_ra` parsing offset drifts are permanently resolved via bytes-based strict extraction. `jvd_hc` is ingested via `SLOP` dataspec.
> - **Governance**: Fully automated declarative testing using `elementary-data`, `soda-core`, and `sqlfluff` within the dbt DAG. Legacy Python-based audit stubs are deprecated.
> - **Inference Topology**: Residual Pandas-based aggregations have been ported into DB-native dbt models.
> - **Execution**: Bucket Relay queue maintains zero-drift health.
> Please refer to the current `HANDOVER_NOTE.md` and Phase 439 metadata for the absolute source of operational truth.


## 1. A3 コア憲法 (13条)
いかなる開発フェーズ・指示においても、以下のルールは絶対的な前提として機能します。

### 1. データの正当性至上主義
データが正しくなければシステムは砂上の楼閣。即座に入力から洗い直し。

### 2. コンテキスト肥大化の防止
コンテキストが増大したら即刻タスクを小分けし、引き継ぎ書と申し送り事項をまとめる。

### 3. 絶対的な DB ファースト (Fat DB / Thin Python)
徹底したDBファースト。コードだけでなくあらゆる情報をDBを活用して管理。Phase 398以降、推論チェーン全体がdbt DAG管理下。

### 4. プロセス不沈 (No-Kill) の原則
RUNボタンを押す負荷が非常に高い。絶対にsys.exit()修正はしない。エラーキャッチして自動復旧。

### 5. 先祖返りとアーキテクチャ破壊の絶対防止
トポロジー的・ファジィな重複スライスの単純化は「システムの破壊行為」。

### 6. 直交特徴の探求と外部知見の吸収
「タケツベ」情報などは例外的にA3にどんどん取り込んで最適化。

### 7. 外部ツールの徹底管理と活用
ローカルLLMや無料LLM枠など、DBで管理。

### 8. 一貫テストとログの精査
機能修正後にバックテストを行い、矛盾があれば修正。

### 9. PC-KEIBA運用の永久凍結とJRA-VAN直接連携
JV-Link等からA3のPythonパイプラインで直接取り込むアーキテクチャのみを正とする。

### 10. 複雑度の不可逆的増大の禁止と使い捨てスクリプトの厳禁
test_*.py, debug_*.py等の使い捨てスクリプト作成・放置は厳禁。Pythonファイル数上限300。

### 11. ガードレール自己保護（メタ防御）
B-06〜B-09チェック項目の削除・無効化は「システム破壊行為」。

### 12. データ資産の不可侵
tyb_parsed, sed_parsed, a3_h1_*, 学習用MATVIEW, 多層学習構造の削除禁止。

### 13. True L3 アーキテクチャの死守
50層・独立馬券種(6モデル)・トポロジー適応を絶対に単一スコアに集約しない。

## 2. BR移譲義務（バケツリレー・ファースト原則）
- SQL_EXEC/AUDIT/HEALTH_CHECK等で自動化できるものは全てBRに投入
- 軽微なコード修正は `LLM_PROMPT` タスクとしてBR投入
- BRの機能で対応できない作業がある場合、BR自体の強化を優先

## 3. セッション開始プロトコル
```bash
docker exec a3-postgres-15 psql -U postgres -d pckeiba -c "SELECT * FROM a3_meta.governance_kickstart(N, 'AgentName');"
```
→ 全PASSになるまで本作業を開始しない。

*Phase 420 / Core Constitution v13 / Fat DB/Thin Python確立済*
