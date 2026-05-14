# A3 Sovereign Architecture - 03. データインジェスション（情報収集パイプライン）

> [!NOTE]
> **Phase 439 Update Notice (2026-05-13)**
> This document was originally established in Phase 306. As of Phase 439, the A3 Sovereign Pipeline has fully transitioned to the **"Fat DB / Thin Python"** architecture.
> - **Data Ingestion**: JVD `jvd_ra` parsing offset drifts are permanently resolved via bytes-based strict extraction. `jvd_hc` is ingested via `SLOP` dataspec.
> - **Governance**: Fully automated declarative testing using `elementary-data`, `soda-core`, and `sqlfluff` within the dbt DAG. Legacy Python-based audit stubs are deprecated.
> - **Inference Topology**: Residual Pandas-based aggregations have been ported into DB-native dbt models.
> - **Execution**: Bucket Relay queue maintains zero-drift health.
> Please refer to the current `HANDOVER_NOTE.md` and Phase 439 metadata for the absolute source of operational truth.


## 1. 概要
A3の「入力」を司るパイプライン群。JRDBデータに加え、JRA-VAN Data Lab直接連携、AI定性データの定量化、外部AIスコアのスクレイピング、過去必勝法則のDNA化を行います。すべてのデータはDBへ統合され、dbtモデルの特徴量としてマッピングされます。

## 2. JRA-VAN Direct Ingestion (JV-Link COM)
PC-KEIBA等の中間DBを完全廃止（Core Constitution #9）。32bit Python COMで直接データ取得。

### 2.1 `a3_jravan_auto_ingester.py` (Phase 389実装)
- **Windows側で実行** (32bit Python 3.11 + JVDTLab.JVLink COM)
- Docker内では実行不可（Windows COM依存）
- JVD固定長文字列の自動パース → PostgreSQL直接UPSERT (SAVEPOINT対応)

### 2.2 dataspec×option 利用可能マトリクス (Phase 419解明)
| dataspec | opt1 | opt2 | opt3 | opt4 | 主な取得データ |
|----------|------|------|------|------|-------------|
| RACE | ✅ | ✅ | ✅ | ✅ | RA/SE/HR/O1-O6 (レース情報) |
| DIFF | ❌ | ❌ | ✅ | ✅ | **UM/KS/CH** (マスター差分) |
| TOKU | ✅ | ✅ | ✅ | ✅ | 特別登録 |
| MING | ✅ | ❌ | ✅ | ✅ | データマイニング |
| WOOD | ✅ | ❌ | ✅ | ✅ | 調教 |
| BLOD/HOSE | ❌ | ❌ | ✅ | ✅ | 血統/馬体重等 |

### 2.3 取込実績 (Phase 420時点)
| テーブル | 件数 | Phase |
|---------|------|-------|
| jvd_se (成績) | **2,853,079** | 初期+Phase 408 PK修正+Phase 419追加 |
| jvd_um (競走馬) | **198,212** 🎉 | Phase 419 (DIFF opt3) |
| jvd_ks (騎手) | **1,493** 🎉 | Phase 419 (DIFF opt3) |
| jvd_ch (調教師) | **1,431** 🎉 | Phase 419 (DIFF opt3) |
| jvd_ra (レース) | 72 | Phase 403 |
| jvd_hr (払戻) | 72 | Phase 403 |
| jvd_o1~o6 (オッズ) | 各種 | Phase 382 |
| jvd_hc (調教) | 0 | DIFF未含有、別手段調査中 |

## 3. JRDB データパイプライン
出馬表(KYI)、馬場状態(BAC)、直前情報(TYB)のパース処理。

- **`a3_pipeline_daily_jrdb.py`:** 固定長テキストをパースして `kyi_parsed_v51`, `bac_parsed`, `tyb_parsed` 等へインサート
- TYB: 馬体重、パドック点、直前オッズ → 推論の動的特徴量（16カラム）

## 4. レース後自動パイプライン (BR統合)
- **Phase 394でBR化完了:** SED_DL → SED_Ingest → JVLink_Ingest → Payouts の依存チェーン
- `a3_gut_shield_v2.py` によるデータ保護
- `a3_payout_aggregator.py` でポートフォリオ照合

## 5. TakeTube パイプライン (YouTube AI Insight Extraction)
- **`a3_taketube_pipeline_v52.py`:** YouTube字幕→Groq/DeepSeek→JSON（本命・穴馬・展開有利不利）
- 出力: `api.a3_taketube_insights` → 推論ビューへの直交特徴量

## 6. VictGrab パイプライン (外部AIスコア)
- **`scrape_victgrab_safe.py`:** Playwrightでスクレイピング
- 二段構え保護（テンポラリ→検証→マスターマージ）

## 7. Heritage DNA パイプライン
- **`a3_heritage_compiler.py`:** 過去SQLの必勝条件マッチフラグ → `feat_heritage_golden_match`
- jvd_um取込済みのため、次セッションでv316スタブ解消可能

## 8. クッション値の自動取得
- **`jra_cushion_scraper_fixed.py`:** JRA公式からスクレイピング → `public.a3_jra_cushion`

*Phase 420 / JVD 13/16テーブル / jvd_um 198,212件 / DIFF opt3突破*
