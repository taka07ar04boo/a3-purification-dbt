# A3 Sovereign Architecture - 04. 特徴量エンジニアリングとデータベース層

> [!NOTE]
> **Phase 439 Update Notice (2026-05-13)**
> This document was originally established in Phase 306. As of Phase 439, the A3 Sovereign Pipeline has fully transitioned to the **"Fat DB / Thin Python"** architecture.
> - **Data Ingestion**: JVD `jvd_ra` parsing offset drifts are permanently resolved via bytes-based strict extraction. `jvd_hc` is ingested via `SLOP` dataspec.
> - **Governance**: Fully automated declarative testing using `elementary-data`, `soda-core`, and `sqlfluff` within the dbt DAG. Legacy Python-based audit stubs are deprecated.
> - **Inference Topology**: Residual Pandas-based aggregations have been ported into DB-native dbt models.
> - **Execution**: Bucket Relay queue maintains zero-drift health.
> Please refer to the current `HANDOVER_NOTE.md` and Phase 439 metadata for the absolute source of operational truth.


## 1. 概要
A3のデータ統合哲学は「Fat DB / Thin Python」— 複雑な結合・変換はPythonではなくdbt(SQL)に任せる。Phase 398以降、推論チェーン全体がdbt DAG管理下に入り、PythonはDB計算済みカラムの参照のみに徹する。

## 2. dbt DAG階層 (81モデル全PASS)

```
=== Staging (stg_*) ===
stg_jrdb_kyi (JRDB出馬表パース)
stg_jrdb_sed (JRDB成績パース)  
stg_ne_rankings (VictGrab外部AI)
stg_jra_horse (jvd_um 198,212件)
stg_jra_training (jvd_hc)
stg_jra_hr/ks/ch (払戻/騎手/調教師 固定長パース)

=== Intermediate (int_*) ===
l1_micro: int_horse_fixed, int_horse_race_history, int_horse_point_condition
l2_context: int_course_static, int_day_track_bias
l3_bias: トラックバイアス補正
l4_future: int_logic_adapter_sire_momentum, int_trainer_strategy_acceleration
l5_consilience: int_a3_consilience (五重共鳴スコア)
l6_strategy: int_v2_optimizer (EV→ポートフォリオ構築)
l7_evolution: int_a3_physical_standard/index, int_hierarchical_causality

=== Mart ===
a3_features_today_base (259カラム — 学習・推論マスタ)

=== Inference (推論チェーン — 全dbt管理) ===
v_inference_today → v53 → v55 → v56 → v316 (is_chaos/regime判定含む)
```

## 3. 統合マスタービュー (`a3_features_today_base` — 259カラム)
過去数年分の全レース情報を完全結合した、A3の「学習マスタ（Source of Truth）」。

### 3.1 カラム構成
- **基礎情報:** 距離、枠番、馬番、斤量、性齢
- **スピード指数系:** IDM、テン指数、上がり指数、ペース指数
- **直前情報 (TYB):** パドック点、馬体重増減、直前オッズ (16カラム)
- **環境変数:** クッション値、含水率、馬場バイアス
- **直交特徴量:** TakeTube LLMスコア、H1歴史知識
- **Heritage DNA:** `feat_heritage_golden_match` (jvd_um取込済、v316最後のスタブ1件)
- **因果推論:** `feat_causal_uplift` 
- **速度微分:** `feat_speed_derivative_*` 
- **血統シナジー:** `feat_bloodline_synergy_*`
- **トポロジー:** `feat_topological_chaos`, `feat_mad_pace_resistance`
- **レース内統計:** `feat_race_std_*`, `feat_race_mean_*`, `feat_*_mad`

### 3.2 v316 スタブ残件
- **残り1件のみ:** `feat_heritage_golden_match` (jvd_umデータ取込済みのため解消可能)
- Phase 401で12件中11件をWINDOW関数で実計算値に置換

## 4. Chaos検知と Regime判定 (v316)
- **is_chaos:** `topological_chaos > 0.65 AND mad_pace_resistance > 1.5` (Phase 405/414/415で修正)
- **閾値管理:** `a3_meta.chaos_thresholds` テーブルで一元管理 (SSOT)
- **Regime:** Core_Sprint_Turf, Core_Mile_Turf, ... Chaos_Turf, Chaos_Dirt 等15種

## 5. Leakage Gate (OOT・未来情報の漏洩防止機構)
- 推論ビューに sed_parsed の「タイム」や「着順」がJOINされていないかを検証
- Preflightで Leakage Gate == 0件 でなければ推論は実行不可

## 6. Source Freshness (dbt)
- `kyi_parsed`, `sed_parsed` に対するingested_atベースの鮮度チェック (warn 7日 / error 14日)
- **2/2 PASS**

*Phase 420 / 259カラム / dbt 81モデル / 推論チェーン全dbt管理 / v316スタブ残1件*
