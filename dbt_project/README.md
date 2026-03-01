# dbt ハンズオン学習プロジェクト

既存の Snowflake Medallion Architecture パイプライン（`sql/` の 17 本の SQL）を dbt に移行するハンズオンプロジェクト。

## 前提条件

- Snowflake アカウントが稼働中で、Bronze テーブルにデータがロード済み
- Python 3.9+ がインストール済み
- `sql/01_database_setup.sql` 〜 `sql/09_initial_load.sql` が実行済み

## アーキテクチャ

```
                     Snowflake ネイティブ（変更なし）
                    ┌─────────────────────────────────────┐
GCS (JSONL.gz) ───→ │ COPY INTO → Bronze テーブル (VARIANT) │
                    │ DAILY_LOAD タスク (CRON 01:00 JST)    │
                    └──────────────┬──────────────────────┘
                                   │
                     dbt で管理     ▼
                    ┌─────────────────────────────────────┐
                    │ staging (= Silver 層)                │
                    │   stg_members ............... view   │
                    │   stg_transactions ......... incr.   │
                    │   stg_daily_member_summary . incr.   │
                    ├─────────────────────────────────────┤
                    │ marts (= Gold 層)                    │
                    │   mart_monthly_active_members        │
                    │   mart_monthly_sales_summary         │
                    │   mart_member_purchase_summary       │
                    └─────────────────────────────────────┘
```

---

## セットアップ

```bash
# 1. プロジェクトディレクトリに移動
cd dbt_project

# 2. Python 仮想環境を作成・有効化
python3 -m venv .venv
source .venv/bin/activate

# 3. dbt-snowflake をインストール
pip install dbt-snowflake

# 4. 環境変数を設定
export SNOWFLAKE_USER=your_username
export SNOWFLAKE_PASSWORD=your_password

# 5. 接続テスト
dbt debug

# 6. パッケージをインストール
dbt deps
```

---

## 学習チャプター

### Chapter 1: プロジェクトセットアップ
`dbt_project.yml`, `profiles.yml`, `packages.yml` の設定を理解する。

### Chapter 2: Sources
`models/sources.yml` — `source()` 関数で Bronze 層テーブルを参照。`dbt source freshness` で鮮度チェック。

### Chapter 3: 最初のモデル — stg_members
`models/staging/stg_members.sql` — view として Silver 層を作成。VARIANT → 型付きカラムへの変換。

### Chapter 4: ref() と DAG
`models/marts/mart_monthly_active_members.sql` — `ref()` でモデル間の依存関係を定義。`dbt docs serve` でリネージグラフを確認。

### Chapter 5: Tests
`models/staging/_staging_models.yml` + `tests/assert_no_future_dates.sql` — スキーマテストとカスタムテスト。

### Chapter 6: Incremental モデル
`models/staging/stg_transactions.sql` — `materialized='incremental'` で差分更新。`unique_key` と `merge_update_columns` の使い方。

### Chapter 7: Marts (Gold 層)
`models/marts/` — ref() チェーン、JOIN、集計関数。

### Chapter 8: ドキュメント
`_staging_models.yml`, `_mart_models.yml` — `dbt docs generate && dbt docs serve` でデータカタログ生成。

### Chapter 9: Macros
`macros/` + `models/staging/*_macro.sql` — 共通パターンのマクロ化。`convert_mongo_timestamp`, `generate_ym_ymd`。

### Chapter 10: Snapshots (SCD Type 2)
`snapshots/snap_members.sql` — 会員データの変更履歴を自動記録。

### Chapter 11: Snowflake 内 dbt 実行
`sql/18〜20` — Git Integration → dbt Project → Task チェーンで Snowflake ネイティブ実行。

---

## よく使うコマンド

```bash
dbt run                              # 全モデル実行
dbt run --select stg_members         # 特定モデルのみ
dbt run --select +mart_monthly_sales_summary  # 依存モデルも含めて実行
dbt test                             # 全テスト実行
dbt snapshot                         # スナップショット実行
dbt docs generate && dbt docs serve  # ドキュメント生成・閲覧
dbt compile --select stg_members     # コンパイル済み SQL を確認
dbt source freshness                 # ソース鮮度チェック
dbt run --full-refresh               # フルリフレッシュ（incremental テーブル再作成）
```
