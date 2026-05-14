# A3 Sovereign Architecture - 13. データ保護とシークレット管理

> [!NOTE]
> **Phase 439 Update Notice (2026-05-13)**
> This document was originally established in Phase 306. As of Phase 439, the A3 Sovereign Pipeline has fully transitioned to the **"Fat DB / Thin Python"** architecture.
> - **Data Ingestion**: JVD `jvd_ra` parsing offset drifts are permanently resolved via bytes-based strict extraction. `jvd_hc` is ingested via `SLOP` dataspec.
> - **Governance**: Fully automated declarative testing using `elementary-data`, `soda-core`, and `sqlfluff` within the dbt DAG. Legacy Python-based audit stubs are deprecated.
> - **Inference Topology**: Residual Pandas-based aggregations have been ported into DB-native dbt models.
> - **Execution**: Bucket Relay queue maintains zero-drift health.
> Please refer to the current `HANDOVER_NOTE.md` and Phase 439 metadata for the absolute source of operational truth.


## 1. データ保護プロトコル

### 1.1 VictGrab (Phase 176教訓)
- 二段構え保護: テンポラリ→検証→マスターマージ
- DBトリガーによるスナップショット義務化
- `scrape_victgrab_safe.py` (安定化済みv7ベース)
- neテーブルのVGカラム操作時は必ずKI参照

### 1.2 JVDデータ保護
- SAVEPOINT対応: UPSERTエラーをレコード単位に隔離 (Phase 403)
- PK制約: Phase 408でketto_toroku_bango追加、umaban INTEGER型キャスト
- 不良データパージ: Phase 404でオフセット修正前データ16件削除

### 1.3 学習データ資産 (Core Constitution #12)
- 削除禁止対象: tyb_parsed, sed_parsed, a3_h1_*, 学習MATVIEW, 多層学習構造

## 2. シークレット管理 (Pattern 65)

### 2.1 DB一元管理
- 環境変数・APIキーは全てDBテーブルで管理
- `.env`の消失による機能不全を防止
- `a3_meta.a3_secrets` テーブル

### 2.2 JRA-VAN ライセンスキー
- Windows環境変数 `$env:JRAVAN_LICENSE_KEY` で渡す
- 32bit Python COM実行時のみ必要

## 3. SHA256 Fingerprinting
- コアファイル7個のSHA256を保存
- `a3_governance.py --preflight` のJ-04チェックで無断改変を検出

*Phase 420 / SAVEPOINT対応済 / DB一元管理*
