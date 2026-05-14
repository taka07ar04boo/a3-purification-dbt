# A3 Sovereign Architecture - 14. 戦略的憲法とJRDBゲノム

> [!NOTE]
> **Phase 439 Update Notice (2026-05-13)**
> This document was originally established in Phase 306. As of Phase 439, the A3 Sovereign Pipeline has fully transitioned to the **"Fat DB / Thin Python"** architecture.
> - **Data Ingestion**: JVD `jvd_ra` parsing offset drifts are permanently resolved via bytes-based strict extraction. `jvd_hc` is ingested via `SLOP` dataspec.
> - **Governance**: Fully automated declarative testing using `elementary-data`, `soda-core`, and `sqlfluff` within the dbt DAG. Legacy Python-based audit stubs are deprecated.
> - **Inference Topology**: Residual Pandas-based aggregations have been ported into DB-native dbt models.
> - **Execution**: Bucket Relay queue maintains zero-drift health.
> Please refer to the current `HANDOVER_NOTE.md` and Phase 439 metadata for the absolute source of operational truth.


## 1. Pattern-First ML哲学
A3の機械学習は「モデルの精度を上げる」のではなく「パターンを見つける」ことに主眼を置きます。競馬予想AIの競争優位性は、データソースの多様性と、そこから抽出されるパターンの質にあります。

## 2. MetaData Genome (34+ JRDBレイアウト)
JRDBが提供する34以上のデータレイアウト（KYI, TYB, SED, BAC, CYB, KKA等）を「ゲノム」として位置づけ、各レイアウトの固有情報を特徴量として体系的に統合。

### 2.1 ゲノム統合状況 (Phase 420)
- KYI (出馬表): ✅ 完全統合 (kyi_parsed_v51)
- TYB (直前情報): ✅ 16カラム統合
- SED (成績): ✅ 完全統合 (sed_parsed)
- BAC (馬場): ✅ 完全統合 (bac_parsed)
- JVD (JRA-VAN): ✅ 13/16テーブル (jvd_se 285万件, jvd_um 19.8万件等)

## 3. 直交特徴量戦略
- TakeTube (YouTube AI): 動画コンテンツからのLLM抽出
- H1 (歴史知識): 17,915チャンクembedding済み
- VictGrab (外部AI): 競合AIのスコアを逆利用
- Heritage DNA: 過去の必勝法則を動的フラグ化

## 4. 差別化要因
- **データ多様性:** JRDB + JRA-VAN + YouTube + 外部AI + 歴史知識
- **Fat DB:** 推論チェーン全体がSQL/dbt管理 → 再現性・監査性
- **自律運用:** BR 636K+タスク完了の自動オーケストレーション
- **Chaos適応:** 波乱レース専用モデルへの動的ルーティング

*Phase 420 / 259カラム / 13 JVDテーブル / 直交特徴量4系統*
