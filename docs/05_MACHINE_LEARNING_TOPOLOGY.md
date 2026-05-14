# A3 Sovereign Architecture - 05. 機械学習トポロジー (ML Topology & Stacking)

> [!NOTE]
> **Phase 439 Update Notice (2026-05-13)**
> This document was originally established in Phase 306. As of Phase 439, the A3 Sovereign Pipeline has fully transitioned to the **"Fat DB / Thin Python"** architecture.
> - **Data Ingestion**: JVD `jvd_ra` parsing offset drifts are permanently resolved via bytes-based strict extraction. `jvd_hc` is ingested via `SLOP` dataspec.
> - **Governance**: Fully automated declarative testing using `elementary-data`, `soda-core`, and `sqlfluff` within the dbt DAG. Legacy Python-based audit stubs are deprecated.
> - **Inference Topology**: Residual Pandas-based aggregations have been ported into DB-native dbt models.
> - **Execution**: Bucket Relay queue maintains zero-drift health.
> Please refer to the current `HANDOVER_NOTE.md` and Phase 439 metadata for the absolute source of operational truth.


## 1. 概要
A3の予測エンジンは、競馬という極めてノイズの多い非線形な事象を予測するため、ドメインごとに専門化された「Mixture of Experts (MoE)」と多層スタッキング構造を採用。Phase 380でV2アーキテクチャに完全切替完了。アーキテクチャの破壊（トポロジーの単純化）はコア憲法によって厳しく禁じられています。

## 2. V2 推論アーキテクチャ (Phase 380 本番切替完了)

### 2.1 推論パイプライン
```
a3_features_today_base (259カラム, dbt mart)
    ↓
a3_inference_chokuzen_v2.py (V2 完全統合済)
    ├── Layer 1: Pre-compute (6馬券種CatBoost) ✅
    ├── Layer 2: Multiplier (Ridge, feat_fu2) ✅
    ├── Layer 3: Veto (dbt + Python動的) ✅
    └── Layer 4: Optimizer (int_v2_optimizer.sql, dbt化) ✅
         ↓
    api.a3_inference_logs → api.a3_portfolios
         ↓
    分析結果出力
```

### 2.2 V2 Bスプリント完了状況 (全10タスク完了)
| タスク | 内容 | 状態 |
|---|---|---|
| B-1~B-6 | V2ドライバ、UT、L1/L2実装 | ✅ 全完了 |
| B-7 | Layer 4 Optimizer (dbt化) | ✅ Phase 379 |
| B-8 | Python動的Veto + UT-L3 | ✅ |
| B-9 | E2E V2推論テスト | ✅ Phase 380 |
| B-10 | V1→V2 本番切替 | ✅ Phase 380 |

## 3. 15の専門家モデル (Regimes / MoE)
- **Core (主流):** Core_Sprint_Turf, Core_Mile_Turf, Core_Middle_Turf, Core_Long_Turf, Core_Sprint_Dirt, Core_Mile_Dirt, Core_Route_Dirt
- **Chaos (波乱傾向):** Chaos_Turf, Chaos_Dirt
  - **Chaos検知:** `topological_chaos > 0.65 AND mad_pace_resistance > 1.5` (Phase 405修正)
  - **閾値SSOT:** `a3_meta.chaos_thresholds` テーブルで一元管理 (Phase 415)
- **Hurdle (障害):** Hurdle
- `a3_router.py` が本日のレースを動的にルーティング（DB-First強制, Phase 398）

## 4. 50層のメタスタッキング構造

### 4.1 レイヤー1 (Base Models / L1)
- CatBoost / LightGBM / XGBoost (GBDT系)
- TabNet (Attentionベース Deep Learning)
- Ridge Regression / Survival Model
- 6馬券種(WIN/UMAREN/UMATAN/WIDE/PLACE/WAKUREN)それぞれに独立モデル

### 4.2 レイヤー2 (Meta Learners / L2)
- L1出力 + 環境変数を統合
- コンテキスト依存重み付け

### 4.3 レイヤー3 (Final Blender / L3 & Portfolio Optimizer)
- 5つの馬券種ごとに独立した確率ゲート（`l3_prob_*`）
- EVゲート: インプライド・プロバビリティ vs 実オッズの乖離でフィルタ
- **枠連等のアービトラージ:** 独立確率から数学的合成

## 5. Fat DB / Thin Python 化推進
- Phase 398: `a3_router.py` DB-First強制（`api.tmp_training_ready`からのみデータロード）
- Phase 399: `a3_inference_chokuzen.py` -97行（z-score/race_std/env/chaos/TYBのPandas再計算廃止）
- Phase 410: `v_inference_today` (18,315文字) dbt化完了
- **残課題:** `a3_router.py` (38箇所Pandas使用) の段階的SQL移行

*Phase 420 / V2完全切替済 / L1-L4全層稼働 / Fat DB推進中*
