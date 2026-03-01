# dbt プロジェクト — ハンズオン学習ガイド

既存の Snowflake Medallion Architecture パイプライン（`sql/` の SQL スクリプト）で構築した Bronze → Silver → Gold のデータ変換を、**dbt（data build tool）で再実装** するハンズオンプロジェクトです。

---

## dbt とは？

**dbt（data build tool）** は、SQL ベースのデータ変換ツールです。

- **SELECT 文を書くだけ** でテーブルやビューを自動作成・管理
- モデル間の **依存関係を自動解決**（DAG = 有向非巡回グラフ）
- **テスト・ドキュメント・バージョン管理** が組み込み
- SQL を知っていれば使える（Python は不要）

### Snowflake ネイティブ SQL との違い

| 観点 | Snowflake ネイティブ SQL | dbt |
|---|---|---|
| テーブル作成 | `CREATE TABLE` + `MERGE` を手書き | `SELECT` 文だけ（dbt が DDL を生成） |
| 依存関係 | Task の `AFTER` で手動管理 | `ref()` で自動解決 |
| 差分更新 | Stream + MERGE を自分で書く | `materialized='incremental'` で宣言的に |
| テスト | 品質チェック SQL を手書き | YAML で宣言 + カスタムテスト |
| ドキュメント | 別途管理 | `dbt docs serve` で自動生成 |
| 変更履歴 | 自分で SCD テーブルを構築 | `dbt snapshot` で自動管理 |

---

## 前提条件

- Snowflake アカウントが稼働中
- `sql/01_database_setup.sql` 〜 `sql/09_initial_load.sql` を実行済み（Bronze 層にデータがある状態）
- Python 3.9 以上がインストール済み

---

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
                    ├─────────────────────────────────────┤
                    │ snapshots                            │
                    │   snap_members (SCD Type 2)          │
                    └─────────────────────────────────────┘
```

**ポイント:** Bronze 層のデータ取り込み（COPY INTO + Task）は Snowflake ネイティブ SQL のまま。dbt は Silver 層以降のデータ変換・集計・テストを担当します。

---

## DAG（依存関係グラフ）

```
Bronze Sources            Staging (Silver)              Marts (Gold)
(dbt 管理外)              (dbt モデル)                  (dbt モデル)
──────────────            ────────────                  ────────────

bronze.members       ──→  stg_members (view)       ──→ mart_member_purchase_summary (table)
                           │
                           └──→ snap_members (snapshot / SCD Type 2)

bronze.transactions  ──→  stg_transactions (incr.) ──→ mart_monthly_sales_summary (table)
                           │
                           └──→ assert_no_future_dates (test)

bronze.daily_member  ──→  stg_daily_member_        ──→ mart_monthly_active_members (table)
  _summary                  summary (incr.)        ──→ mart_member_purchase_summary (table)
```

`dbt docs generate && dbt docs serve` で、ブラウザ上でインタラクティブなグラフを確認できます。

---

## セットアップ

```bash
# 1. プロジェクトディレクトリに移動
cd dbt_project

# 2. Python 仮想環境を作成・有効化
python3 -m venv .venv
source .venv/bin/activate          # macOS / Linux
# .venv\Scripts\activate           # Windows

# 3. dbt-snowflake をインストール
pip install dbt-snowflake

# 4. 環境変数を設定（Snowflake の認証情報）
export SNOWFLAKE_USER=your_username
export SNOWFLAKE_PASSWORD=your_password

# 5. profiles.yml の account を書き換え
#    profiles.yml 内の "your-account-id" を自分のアカウントIDに変更
#    例: xy12345.ap-northeast-1.aws

# 6. 接続テスト
dbt debug

# 7. パッケージをインストール（dbt_utils）
dbt deps

# 8. 全モデル実行
dbt run

# 9. テスト実行
dbt test
```

### `dbt debug` が成功しない場合

| エラー | 対処 |
|---|---|
| `Could not connect to Snowflake` | `SNOWFLAKE_USER` / `SNOWFLAKE_PASSWORD` / `account` を確認 |
| `Database 'RETAIL_DWH' does not exist` | `sql/01_database_setup.sql` を先に実行 |
| `Warehouse 'RETAIL_WH' does not exist` | 同上 |
| `dbt_project.yml not found` | `cd dbt_project` でプロジェクトルートに移動しているか確認 |

---

## プロジェクト構成

```
dbt_project/
├── dbt_project.yml            # プロジェクトのルート設定
├── profiles.yml               # Snowflake 接続情報
├── packages.yml               # 外部パッケージ（dbt_utils）
│
├── models/
│   ├── sources.yml            # Bronze 層テーブルの定義（source）
│   │
│   ├── staging/               # Silver 層モデル
│   │   ├── _staging_models.yml           # テスト・ドキュメント定義
│   │   ├── stg_members.sql               # 会員マスタ（view）
│   │   ├── stg_members_macro.sql         # ↑ のマクロ版（比較用）
│   │   ├── stg_transactions.sql          # 購買トランザクション（incremental）
│   │   ├── stg_transactions_macro.sql    # ↑ のマクロ版（比較用）
│   │   ├── stg_daily_member_summary.sql  # 会員別日次集計（incremental）
│   │   └── stg_daily_member_summary_macro.sql  # ↑ のマクロ版（比較用）
│   │
│   └── marts/                 # Gold 層モデル
│       ├── _mart_models.yml              # テスト・ドキュメント定義
│       ├── mart_monthly_active_members.sql    # 月別アクティブ会員数
│       ├── mart_monthly_sales_summary.sql     # 月別売上集計
│       └── mart_member_purchase_summary.sql   # 会員別購入集計
│
├── macros/                    # 再利用可能な Jinja マクロ
│   ├── convert_mongo_timestamp.sql   # MongoDB $date → JST 変換
│   ├── generate_ym_ymd.sql           # ym / ymd パーティションキー生成
│   └── generate_schema_name.sql      # スキーマ名のカスタマイズ
│
├── snapshots/                 # SCD Type 2（変更履歴の自動記録）
│   └── snap_members.sql              # 会員データの変更履歴
│
└── tests/                     # カスタムテスト
    └── assert_no_future_dates.sql    # 未来日付のトランザクションがないことを検証
```

---

## 学習チャプター

### Chapter 1: プロジェクトセットアップ

**学ぶファイル:** `dbt_project.yml`, `profiles.yml`, `packages.yml`

dbt プロジェクトの基本構成を理解します。

**dbt_project.yml の重要な設定:**

```yaml
models:
  medallion_dbt:
    staging:
      +schema: SILVER_DBT        # → RETAIL_DWH.SILVER_DBT に出力
      +materialized: view        # デフォルトは view（個別に上書き可能）
    marts:
      +schema: GOLD_DBT          # → RETAIL_DWH.GOLD_DBT に出力
      +materialized: table       # デフォルトは table
```

- `+schema` でモデルの出力先スキーマを指定
- `+materialized` でデフォルトの実体化方式を指定
- 個々のモデル内で `{{ config(materialized='incremental') }}` のように上書きできる

**profiles.yml の構成:**

```yaml
medallion_dbt:
  target: dev              # デフォルトのターゲット環境
  outputs:
    dev:                   # ローカル開発用（環境変数で認証）
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      ...
    prod:                  # Snowflake Task 内実行用（RBAC で認証）
      user: 'not needed'   # Snowflake 内実行時は不要
      ...
```

- `dev` / `prod` の 2 つのターゲットを使い分け
- `env_var()` で環境変数を読み込み、SQL に認証情報を書かない
- `threads: 4` で最大 4 モデルを並列ビルド

---

### Chapter 2: Sources — Bronze 層の参照

**学ぶファイル:** `models/sources.yml`

dbt が管理していない外部テーブル（Bronze 層）を `source()` 関数で参照する方法を学びます。

```yaml
sources:
  - name: bronze
    database: RETAIL_DWH
    schema: BRONZE
    freshness:                      # データ鮮度の監視設定
      warn_after: {count: 36, period: hour}
      error_after: {count: 48, period: hour}
    loaded_at_field: LOADED_AT      # 鮮度判定に使うタイムスタンプカラム
    tables:
      - name: members
      - name: transactions
      - name: daily_member_summary
```

**source() の使い方（モデル内で）:**
```sql
SELECT * FROM {{ source('bronze', 'members') }}
-- コンパイル結果: SELECT * FROM RETAIL_DWH.BRONZE.MEMBERS
```

**データ鮮度チェック:**
```bash
dbt source freshness
```
- `LOADED_AT` カラムの最新値を確認し、36 時間以上古ければ警告、48 時間以上なら失敗
- Bronze 層のデータロード（Snowflake Task）が正常に動いているか監視できる

---

### Chapter 3: 最初のモデル — stg_members

**学ぶファイル:** `models/staging/stg_members.sql`

Bronze テーブルの VARIANT 型データを、型付きカラムに変換する Silver 層モデルを作成します。

```sql
-- stg_members.sql
SELECT
  RAW_DATA:"_id"::VARCHAR(100)          AS member_id,
  RAW_DATA:"email"::VARCHAR(5000)       AS email,
  RAW_DATA:"member_name"::VARCHAR(5000) AS member_name,
  RAW_DATA:"gender"::VARCHAR(10)        AS gender,
  -- MongoDB の $date 型タイムスタンプ → UTC → JST 変換
  CONVERT_TIMEZONE('UTC','Asia/Tokyo',
    RAW_DATA:"registered_at":"$date"::TIMESTAMP_NTZ
  )::TIMESTAMP_LTZ                      AS registered_at,
  -- パーティションキーの導出
  TO_CHAR(registered_at_jst, 'YYYYMM')   AS ym,
  TO_CHAR(registered_at_jst, 'YYYYMMDD') AS ymd,
  LOAD_DATE
FROM {{ source('bronze', 'members') }}
```

**ポイント:**
- `materialized = view`（デフォルト設定を継承）。約 1,000 件の少量データなので view で十分
- VARIANT 型からの抽出: `RAW_DATA:"フィールド名"::型`
- MongoDB の `$date` 形式: `RAW_DATA:"field":"$date"::TIMESTAMP_NTZ` でネストを辿る
- タイムゾーン変換: UTC → Asia/Tokyo（JST）
- `{{ source('bronze', 'members') }}` で Bronze テーブルを参照

**Snowflake ネイティブとの比較:**
- ネイティブ SQL: Dynamic Table（`TARGET_LAG = '1 hour'`）として自動リフレッシュ
- dbt: view として定義。常に最新データを返す（ラグなし）

---

### Chapter 4: ref() と DAG

**学ぶファイル:** `models/marts/mart_monthly_active_members.sql`

`ref()` 関数でモデル間の依存関係を定義し、dbt が自動で実行順序を決める仕組みを学びます。

```sql
-- mart_monthly_active_members.sql
SELECT
  ym,
  COUNT(DISTINCT member_id) AS active_member_count
FROM {{ ref('stg_daily_member_summary') }}
WHERE has_purchased = TRUE
GROUP BY ym
```

**ref() の役割:**
1. モデル名から **実際のテーブル/ビュー名を解決**（スキーマを意識しなくてよい）
2. **依存関係を DAG として記録** → `dbt run` 時に正しい順序で実行
3. `dbt docs serve` の **リネージグラフ** に反映

**確認コマンド:**
```bash
# DAG をブラウザで可視化
dbt docs generate && dbt docs serve

# 特定モデルとその上流を一括実行（+ プレフィックス）
dbt run --select +mart_monthly_active_members
```

---

### Chapter 5: Tests — データ品質テスト

**学ぶファイル:** `models/staging/_staging_models.yml`, `tests/assert_no_future_dates.sql`

dbt の 2 種類のテストを学びます。

#### スキーマテスト（YAML で宣言）

`_staging_models.yml` にテスト定義を記述します。

```yaml
models:
  - name: stg_members
    columns:
      - name: member_id
        tests:
          - unique            # 重複がないこと
          - not_null          # NULL がないこと
      - name: is_active
        tests:
          - not_null
          - accepted_values:  # 許容値のリスト
              values: [true, false]
      - name: member_rank
        tests:
          - accepted_values:
              values: ['REGULAR','SILVER','GOLD','PLATINUM']
              config:
                severity: warn   # 失敗しても警告のみ（ビルドを止めない）
```

**4 種類の組み込みテスト:**

| テスト | 意味 | 使いどころ |
|---|---|---|
| `unique` | カラムに重複がない | 主キーやユニークキー |
| `not_null` | NULL がない | 必須カラム |
| `accepted_values` | 値が指定リストに含まれる | カテゴリカラム（gender, rank 等） |
| `relationships` | 他テーブルの値が存在する（外部キー） | テーブル間の参照整合性 |

**severity の使い分け:**
- `error`（デフォルト）: テスト失敗でビルド中断
- `warn`: テスト失敗でも警告のみ。データ型の不一致など、既知の問題に使用

#### カスタムテスト（SQL ファイル）

`tests/assert_no_future_dates.sql`:
```sql
-- 未来日付のトランザクションが存在しないことを検証
-- 行が返ればテスト失敗
SELECT *
FROM {{ ref('stg_transactions') }}
WHERE transaction_date > CURRENT_TIMESTAMP()
```

**ルール:** テスト SQL は「違反レコード」を返す。0 行 = 成功、1 行以上 = 失敗。

---

### Chapter 6: Incremental モデル — 差分更新

**学ぶファイル:** `models/staging/stg_transactions.sql`

大量データを効率的に処理する **incremental（差分更新）** の仕組みを学びます。

```sql
{{
  config(
    materialized='incremental',
    unique_key='_id_str',
    merge_update_columns=[
      'transaction_id', 'item_index', 'card_number', 'store_code',
      -- ... 更新対象の全カラムを列挙
    ]
  )
}}

WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY RAW_DATA:"_id":"$binary":"base64"::VARCHAR(200)
      ORDER BY LOADED_AT DESC
    ) AS rn
  FROM {{ source('bronze', 'transactions') }}
  {% if is_incremental() %}
    WHERE LOADED_AT > (SELECT MAX(LOADED_AT) FROM {{ this }})
  {% endif %}
)
SELECT
  -- VARIANT からの型変換（省略）
FROM ranked
WHERE rn = 1
```

**仕組み:**

| 回数 | 動作 | SQL |
|---|---|---|
| 初回 | テーブルを新規作成 | `CREATE TABLE AS SELECT ...` |
| 2 回目以降 | 差分のみ処理 | `MERGE INTO ... USING (新規データ) ON unique_key ...` |

**config の各オプション:**
- `materialized='incremental'`: 差分更新モードを有効化
- `unique_key='_id_str'`: MERGE の突合キー（このキーで既存レコードとマッチ）
- `merge_update_columns=[...]`: MATCHED 時に UPDATE するカラムを明示的に指定

**`is_incremental()` の条件分岐:**
```sql
{% if is_incremental() %}
  -- 2回目以降: 前回の最大 LOADED_AT 以降のデータのみ処理
  WHERE LOADED_AT > (SELECT MAX(LOADED_AT) FROM {{ this }})
{% endif %}
-- 初回実行時: この WHERE 句は生成されない（全データを処理）
```

**`{{ this }}` とは:**
- 現在のモデル自身のテーブルを参照する特殊な変数
- incremental モデルで「前回までのデータ」を参照するのに使う

**重複排除（ROW_NUMBER）:**
Bronze 層には同じレコードが複数回入る可能性があるため、`LOADED_AT DESC` で最新のものだけを残します。

**Snowflake ネイティブとの比較:**
- ネイティブ SQL: Stream + MERGE（`SYSTEM$STREAM_HAS_DATA` で条件実行）
- dbt: `incremental` + `is_incremental()` で同等の差分更新を実現

**フルリフレッシュ:**
```bash
# incremental テーブルを再作成（全データ再処理）
dbt run --full-refresh --select stg_transactions
```

---

### Chapter 7: Marts（Gold 層）

**学ぶファイル:** `models/marts/` 配下の 3 ファイル

Silver 層のデータを集計して、ビジネスユーザーが直接分析に使えるテーブルを作成します。

#### mart_monthly_active_members.sql
```sql
SELECT
  ym,
  COUNT(DISTINCT member_id) AS active_member_count
FROM {{ ref('stg_daily_member_summary') }}
WHERE has_purchased = TRUE
GROUP BY ym
```
月ごとに「1 回以上購入した会員数」を集計。

#### mart_monthly_sales_summary.sql
```sql
SELECT
  ym,
  COUNT(DISTINCT card_number) AS buyer_count,
  COUNT(*) AS transaction_count,
  SUM(unit_price * quantity) AS total_sales
FROM {{ ref('stg_transactions') }}
GROUP BY ym
```
月ごとの購入者数・取引件数・総売上を集計。

#### mart_member_purchase_summary.sql
```sql
SELECT
  dms.member_id,
  m.member_name,
  COUNT(*) AS total_days,
  SUM(CASE WHEN dms.has_purchased THEN 1 ELSE 0 END) AS purchase_days,
  SUM(CASE WHEN dms.has_returned THEN 1 ELSE 0 END) AS return_days
FROM {{ ref('stg_daily_member_summary') }} dms
JOIN {{ ref('stg_members') }} m
  ON dms.member_id = CAST(m.member_id AS INTEGER)
GROUP BY dms.member_id, m.member_name
```
会員ごとの集計日数・購入日数・返品日数。**2 つの staging モデルを JOIN** する例。

> `CAST(m.member_id AS INTEGER)` は、stg_members の member_id が VARCHAR（VARIANT から抽出）で、stg_daily_member_summary の member_id が INTEGER のため。

**全 mart モデルは `materialized = table`**（`dbt_project.yml` の marts デフォルト）。`dbt run` のたびに DROP + CREATE TABLE AS SELECT で再作成されます。

---

### Chapter 8: ドキュメント

**学ぶファイル:** `_staging_models.yml`, `_mart_models.yml`

YAML ファイルに記述したカラムの説明やテスト定義が、自動でデータカタログになります。

```bash
# ドキュメントサイトを生成・起動
dbt docs generate && dbt docs serve
```

ブラウザで以下が確認できます:
- 全モデルの一覧とカラム定義
- リネージグラフ（DAG の可視化）
- テストの一覧と結果
- ソースの鮮度情報

---

### Chapter 9: Macros — Jinja テンプレートによる再利用

**学ぶファイル:** `macros/` 配下の 3 ファイル + `*_macro.sql` モデル

各 staging モデルには、通常版（`stg_members.sql`）とマクロ版（`stg_members_macro.sql`）の 2 つがあります。比較して、マクロの効果を実感してください。

#### convert_mongo_timestamp マクロ

```sql
-- macros/convert_mongo_timestamp.sql
{% macro convert_mongo_timestamp(variant_col, field_name) %}
  CONVERT_TIMEZONE('UTC', 'Asia/Tokyo',
    {{ variant_col }}:"{{ field_name }}":"$date"::TIMESTAMP_NTZ
  )::TIMESTAMP_LTZ
{% endmacro %}
```

**使用前（通常版）:**
```sql
CONVERT_TIMEZONE('UTC','Asia/Tokyo',
  RAW_DATA:"registered_at":"$date"::TIMESTAMP_NTZ)::TIMESTAMP_LTZ AS registered_at,
CONVERT_TIMEZONE('UTC','Asia/Tokyo',
  RAW_DATA:"last_visit_at":"$date"::TIMESTAMP_NTZ)::TIMESTAMP_LTZ AS last_visit_at,
CONVERT_TIMEZONE('UTC','Asia/Tokyo',
  RAW_DATA:"updated_at":"$date"::TIMESTAMP_NTZ)::TIMESTAMP_LTZ AS updated_at,
```

**使用後（マクロ版）:**
```sql
{{ convert_mongo_timestamp('RAW_DATA', 'registered_at') }} AS registered_at,
{{ convert_mongo_timestamp('RAW_DATA', 'last_visit_at') }} AS last_visit_at,
{{ convert_mongo_timestamp('RAW_DATA', 'updated_at') }}    AS updated_at,
```

#### generate_ym_ymd マクロ

```sql
-- macros/generate_ym_ymd.sql
{% macro generate_ym_ymd(variant_col, field_name) %}
  TO_CHAR(CONVERT_TIMEZONE('UTC', 'Asia/Tokyo',
    {{ variant_col }}:"{{ field_name }}":"$date"::TIMESTAMP_NTZ), 'YYYYMM') AS ym,
  TO_CHAR(CONVERT_TIMEZONE('UTC', 'Asia/Tokyo',
    {{ variant_col }}:"{{ field_name }}":"$date"::TIMESTAMP_NTZ), 'YYYYMMDD') AS ymd
{% endmacro %}
```

1 回の呼び出しで `ym` と `ymd` の 2 カラムを生成。

#### generate_schema_name マクロ

dbt のデフォルト動作では、スキーマ名が `PUBLIC_SILVER_DBT` のように **prefix + custom_schema** に結合されます。このマクロでカスタムスキーマ名（`SILVER_DBT`）をそのまま使うように変更しています。

```sql
-- デフォルト動作: PUBLIC_SILVER_DBT（prefix が付く）
-- このマクロ適用後: SILVER_DBT（custom_schema のみ）
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

**コンパイル済み SQL の確認:**
```bash
dbt compile --select stg_members
# target/compiled/ にコンパイル後の SQL が出力される
# マクロがどう展開されたか確認できる
```

---

### Chapter 10: Snapshots（SCD Type 2）

**学ぶファイル:** `snapshots/snap_members.sql`

会員データの変更履歴を自動で記録する仕組み（Slowly Changing Dimension Type 2）を学びます。

```sql
{% snapshot snap_members %}
{{
  config(
    target_schema='SNAPSHOTS',
    unique_key='member_id',
    strategy='timestamp',
    updated_at='updated_at'
  )
}}
SELECT * FROM {{ ref('stg_members') }}
{% endsnapshot %}
```

**仕組み:**
1. 初回の `dbt snapshot`: 全レコードを `SNAPSHOTS.SNAP_MEMBERS` テーブルに挿入
2. 2 回目以降: `member_id` で突合し、`updated_at` が変わったレコードを検知
3. 旧レコードに `dbt_valid_to`（終了日時）をセット
4. 新レコードを `dbt_valid_from`（開始日時）付きで挿入

**dbt が自動追加するカラム:**

| カラム | 説明 |
|---|---|
| `dbt_scd_id` | 履歴レコードの一意キー |
| `dbt_updated_at` | 変更検知に使われたタイムスタンプ |
| `dbt_valid_from` | このバージョンが有効になった日時 |
| `dbt_valid_to` | このバージョンが無効になった日時（NULL = 現在有効） |

**ユースケース:** 「この会員のランクはいつ GOLD に昇格したか？」「住所変更の履歴は？」といった履歴分析が可能。

```bash
dbt snapshot   # スナップショットを実行
```

---

### Chapter 11: Snowflake 内 dbt 実行

**学ぶファイル:** `sql/18_git_repository.sql`, `sql/19_dbt_project.sql`, `sql/20_dbt_task.sql`

ローカルで `dbt run` する代わりに、**Snowflake の Task から直接 dbt を実行** する方法を学びます。

**構成:**
```
GitHub リポジトリ
    ↓ Git Integration (sql/18)
Snowflake Git Repository
    ↓ dbt Project (sql/19)
Snowflake dbt Project オブジェクト
    ↓ Task (sql/20)
DAILY_LOAD → DBT_BUILD（dbt build --target prod）
```

- `profiles.yml` の `prod` ターゲットを使用（Snowflake 内実行時は認証不要）
- `dbt build` = モデル実行 + テスト + スナップショットを一括実行
- ローカルの `dbt run` と同じロジックが Snowflake 内で自動実行される

詳細な手順は `sql/README.md` の Phase S（18〜20）を参照してください。

---

## よく使うコマンド

### 基本コマンド

```bash
dbt run                              # 全モデル実行
dbt test                             # 全テスト実行
dbt build                            # run + test + snapshot を依存関係順に実行
dbt snapshot                         # スナップショット実行
dbt source freshness                 # ソース鮮度チェック
```

### モデル選択

```bash
dbt run --select stg_members                  # 特定モデルのみ
dbt run --select +mart_monthly_sales_summary  # そのモデル + 上流の全モデル
dbt run --select staging.*                    # staging フォルダ内の全モデル
dbt run --select tag:daily                    # タグ指定（タグ付きの場合）
```

### デバッグ・確認

```bash
dbt debug                            # Snowflake 接続テスト
dbt compile --select stg_members     # コンパイル済み SQL を確認（実行はしない）
dbt docs generate && dbt docs serve  # ドキュメントサイト生成・閲覧
dbt run --full-refresh               # incremental テーブルを全再作成
dbt ls                               # プロジェクト内の全リソースを一覧表示
```

### コマンドの使い分け

| やりたいこと | コマンド |
|---|---|
| 初回セットアップ | `dbt deps` → `dbt run` → `dbt test` |
| 日常の開発 | `dbt run --select <model>` → `dbt test --select <model>` |
| 本番デプロイ | `dbt build`（全モデル + テスト + スナップショット） |
| incremental が壊れた | `dbt run --full-refresh --select <model>` |
| データが来ているか確認 | `dbt source freshness` |
| SQL の動作確認 | `dbt compile --select <model>` で target/ を確認 |

---

## dbt 主要概念のまとめ

| 概念 | 説明 | 使用箇所 |
|---|---|---|
| **source()** | dbt 管理外テーブルの参照 | `sources.yml` → 全 staging モデル |
| **ref()** | dbt モデル間の依存参照（DAG 構築） | marts が staging を参照 |
| **materialized** | 実体化方式（view / table / incremental） | `dbt_project.yml` + 個別モデル |
| **incremental** | 差分更新（MERGE）。大量データ向け | `stg_transactions`, `stg_daily_member_summary` |
| **unique_key** | incremental の MERGE 突合キー | `_id_str`, `_id` |
| **is_incremental()** | 2 回目以降の実行かを判定する Jinja 関数 | 差分フィルタの WHERE 句 |
| **{{ this }}** | 現在のモデル自身のテーブルを参照 | incremental の MAX(LOADED_AT) 取得 |
| **macro** | 再利用可能な Jinja テンプレート | `convert_mongo_timestamp` 等 |
| **schema test** | YAML 宣言型テスト（unique, not_null 等） | `_staging_models.yml` |
| **singular test** | SQL ファイルによるカスタムテスト | `tests/assert_no_future_dates.sql` |
| **snapshot** | SCD Type 2 の変更履歴自動記録 | `snap_members.sql` |
| **freshness** | ソーステーブルのデータ鮮度監視 | `sources.yml` |
| **generate_schema_name** | スキーマ命名のカスタマイズ | `macros/generate_schema_name.sql` |
