# Snowflake ネイティブ SQL スクリプト

GCS（Google Cloud Storage）から Snowflake へのメダリオンアーキテクチャ（Bronze / Silver / Gold）パイプラインを、**Snowflake ネイティブ SQL のみ** で構築するスクリプト群です。

Snowsight（Snowflake の Web UI）で **01 → 02 → … → 20** の番号順に実行してください。

---

## 全体の流れ

```
Phase A-E   インフラ構築          01〜05
Phase F     監視テーブル          06
Phase G-I   Bronze 層の構築       07〜09
Phase K-M   Silver 層の構築       10〜12
Phase N-O   自動化 + Gold 層      13〜15
Phase P-R   Task 運用             16〜17
Phase S     dbt 連携（オプション） 18〜20
```

### 処理フローの概要

```
GCS (JSONL.gz)
  │  ← Storage Integration + External Stage
  ▼
Bronze（VARIANT 型で生 JSON をそのまま格納）
  │  ← Stream が INSERT/UPDATE/DELETE を検知
  ▼
Silver（型変換・タイムゾーン変換・正規化済み）
  │  ← Dynamic Table が自動リフレッシュ
  ▼
Gold（ビジネス集計テーブル）
```

---

## ファイル一覧と詳細解説

### Phase A-E: インフラストラクチャ構築（01〜05）

#### 01_database_setup.sql — DB / Schema / Warehouse の作成

メダリオンアーキテクチャの土台となるオブジェクトを作成します。

| 作成オブジェクト | 名前 | 説明 |
|---|---|---|
| Database | `RETAIL_DWH` | プロジェクト全体の格納先 |
| Schema | `BRONZE` | 生データ層 |
| Schema | `SILVER` | クレンジング済みデータ層 |
| Schema | `GOLD` | ビジネス集計層 |
| Schema | `MONITORING` | 運用監視用 |
| Warehouse | `RETAIL_WH` | クエリ実行用仮想ウェアハウス |

**ポイント:**
- `WAREHOUSE_SIZE = 'XSMALL'` で最小（最安）のウェアハウスを使用（ハンズオンでは十分）
- `AUTO_SUSPEND = 60` で 60 秒無操作後に自動停止 → クレジット節約
- `AUTO_RESUME = TRUE` でクエリ実行時に自動起動
- すべて `IF NOT EXISTS` 付きで冪等（何度実行しても安全）

---

#### 02_storage_integration.sql — GCS との接続認証

Snowflake が GCS バケットにアクセスするための **Storage Integration** を作成します。

```sql
CREATE OR REPLACE STORAGE INTEGRATION GCS_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://your-gcs-bucket-name/');
```

**ポイント:**
- `ACCOUNTADMIN` ロールが必要
- `STORAGE_ALLOWED_LOCATIONS` は自分の GCS バケット URL に書き換える
- 作成後に `DESCRIBE INTEGRATION GCS_INTEGRATION` を実行 → `STORAGE_GCP_SERVICE_ACCOUNT` の値を控える（次の手順で使う）

**なぜ Storage Integration を使うのか:**
- 認証情報（キーやパスワード）が SQL に露出しない
- Snowflake が GCP サービスアカウントを自動生成し、IAM で権限管理できる
- バケットごとの細かいアクセス制御が可能

---

#### 03_iam_setup.sql — GCP 側の IAM 設定

**このファイルは SQL ではなく、GCP 側で行う手順のガイドです。**

前の手順で取得した `STORAGE_GCP_SERVICE_ACCOUNT`（例: `xxxx@gcpuscentral1-1234.iam.gserviceaccount.com`）に対して、GCS バケットへの読み取り権限を付与します。

**GCP Console での操作:**
1. Cloud Storage → 対象バケット → 「権限」タブ
2. 「アクセスを許可」をクリック
3. 新しいプリンシパル: Snowflake サービスアカウントのメールアドレス
4. ロール: `Storage Object Viewer`

**gcloud CLI での操作:**
```bash
gcloud storage buckets add-iam-policy-binding gs://your-bucket \
  --member="serviceAccount:xxxx@gcpuscentral1-1234.iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer"
```

> IAM の反映には数分かかる場合があります。次の手順でエラーが出たら少し待って再実行してください。

---

#### 04_file_format_stage.sql — ファイルフォーマットと外部ステージ

GCS のデータを読み込むための「形式の定義」と「接続先」を作成します。

| 作成オブジェクト | 名前 | 説明 |
|---|---|---|
| File Format | `BRONZE.JSONL_GZ` | JSONL（1行1JSON）の gzip 圧縮ファイル |
| External Stage | `BRONZE.GCS_RAW_DATA` | GCS バケット内の `raw-data/` を参照 |

```sql
-- ファイルフォーマット: 1行1JSONオブジェクト（JSONL）、gzip圧縮
CREATE OR REPLACE FILE FORMAT JSONL_GZ
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = FALSE
  COMPRESSION = 'GZIP';

-- 外部ステージ: Storage Integration + File Format を紐づけ
CREATE OR REPLACE STAGE GCS_RAW_DATA
  STORAGE_INTEGRATION = GCS_INTEGRATION
  URL = 'gcs://your-gcs-bucket-name/raw-data/'
  FILE_FORMAT = JSONL_GZ;
```

**ポイント:**
- `STRIP_OUTER_ARRAY = FALSE`: ファイルは JSON 配列ではなく JSONL 形式（1行ごとに独立した JSON）
- `URL` は自分の GCS バケットに書き換える
- Stage は Bronze スキーマに作成（データ取り込みは Bronze の責務）

---

#### 05_stage_verify.sql — 接続確認

```sql
LIST @BRONZE.GCS_RAW_DATA;
```

GCS バケット内のファイル一覧が表示されれば接続成功です。エラーの場合は:
- IAM 設定の反映待ち（数分程度）
- バケット名や URL の typo
- Storage Integration の `STORAGE_ALLOWED_LOCATIONS` の不一致

をチェックしてください。

---

### Phase F: 運用監視テーブル（06）

#### 06_monitoring_tables.sql — ロードログと品質ログ

ETL パイプラインの運用監視に使うテーブルを 2 つ作成します。

| テーブル | 用途 | 主なカラム |
|---|---|---|
| `MONITORING.LOAD_LOG` | データロードの実行記録 | `COLLECTION_NAME`, `LOAD_TYPE`, `STATUS`, `EXECUTED_AT` |
| `MONITORING.QUALITY_LOG` | データ品質チェックの結果 | `TABLE_NAME`, `ROW_COUNT`, `STATUS`（OK/ALERT）, `CHECKED_AT` |

**なぜ必要か:**
- データパイプラインは「動いているか？」「データは入ったか？」を常に監視する必要がある
- 問題が発生したとき、いつ・どのテーブルで・何が起きたかを追跡できる

---

### Phase G-I: Bronze 層の構築（07〜09）

#### 07_bronze_tables.sql — テーブルと Stream の作成

3 つの Bronze テーブルと、それぞれに対応する Stream を作成します。

**テーブル構造（3 テーブルとも同一）:**

| カラム | 型 | 説明 |
|---|---|---|
| `RAW_DATA` | `VARIANT` | JSON ドキュメントをそのまま格納 |
| `LOAD_DATE` | `DATE` | データの日付（パーティション） |
| `LOADED_AT` | `TIMESTAMP_LTZ` | ロード実行時刻（自動設定） |

**Stream（変更データキャプチャ = CDC）:**

| Stream | 監視対象 |
|---|---|
| `BRONZE.MEMBERS_STREAM` | `BRONZE.MEMBERS` |
| `BRONZE.TRANSACTIONS_STREAM` | `BRONZE.TRANSACTIONS` |
| `BRONZE.DAILY_MEMBER_SUMMARY_STREAM` | `BRONZE.DAILY_MEMBER_SUMMARY` |

**VARIANT 型を使う理由:**
- Bronze 層はソースデータを「そのまま」保存する場所
- スキーマの変更（フィールド追加・名前変更）に強い
- 加工は Silver 層で行う（スキーマオンリード）

**Stream とは:**
- テーブルへの変更（INSERT / UPDATE / DELETE）を自動で追跡する仕組み
- 「前回読んだところからの差分」だけを取得できる
- MERGE 文で読み取ると自動的にオフセットが進む（消費される）

---

#### 08_stored_procedures.sql — データロード用のストアドプロシージャ

GCS から Bronze テーブルにデータを取り込むための 2 つのプロシージャを作成します。

**`BRONZE.LOAD_COLLECTION(COLLECTION_NAME, TARGET_TABLE, LOAD_TYPE, TARGET_DATE)`**

1 つのコレクション（テーブル）をロードする汎用プロシージャ。

```
GCS パスの生成ルール:
  ym=202602/ymd=20260215/members.json.gz
  ym=202602/ymd=20260215/transactions.json.gz
  ym=202602/ymd=20260215/daily_member_summary.json.gz
```

- `LOAD_TYPE = 'FULL'` → `TRUNCATE TABLE` してから `COPY INTO`（全量洗い替え）
- `LOAD_TYPE = 'DAILY'` → `COPY INTO` のみ（追記）
- 実行後に `MONITORING.LOAD_LOG` へ結果を記録

**`BRONZE.LOAD_ALL(TARGET_DATE)`**

3 つのコレクションをまとめてロードするラッパー。

| コレクション | ロードタイプ | 理由 |
|---|---|---|
| `members` | FULL（全量） | 約 1,000 件と少量。毎回洗い替えが安全 |
| `transactions` | DAILY（差分） | 初期 10 万件 + 日次 1 万件。追記で効率化 |
| `daily_member_summary` | DAILY（差分） | 初期 35 万件 + 日次 1 万件 |

**ポイント:**
- `EXECUTE IMMEDIATE` で動的 SQL を組み立て（日付からパスを生成）
- `ON_ERROR = 'ABORT_STATEMENT'` でエラー時に即中断
- `$1` は COPY INTO の SELECT で「ファイル内の JSON 行全体」を指す

---

#### 09_initial_load.sql — 初期データのロード

```sql
CALL BRONZE.LOAD_ALL('2026-02-15'::DATE);
```

初回のデータロードを実行します。日付 `2026-02-15` は GCS 上に存在するデータの日付に合わせてください。

実行後の確認:
```sql
SELECT 'MEMBERS' AS table_name, COUNT(*) AS row_count FROM BRONZE.MEMBERS
UNION ALL
SELECT 'TRANSACTIONS', COUNT(*) FROM BRONZE.TRANSACTIONS
UNION ALL
SELECT 'DAILY_MEMBER_SUMMARY', COUNT(*) FROM BRONZE.DAILY_MEMBER_SUMMARY;
```

期待値: MEMBERS ≒ 1,000 / TRANSACTIONS ≒ 100,000 / DAILY_MEMBER_SUMMARY ≒ 350,000

---

### Phase K-M: Silver 層の構築（10〜12）

Silver 層は Bronze のデータを「型付け・タイムゾーン変換・正規化」して利用しやすくする層です。
データの特性に応じて **Dynamic Table** と **MERGE** を使い分けます。

| テーブル | 方式 | 理由 |
|---|---|---|
| MEMBERS | Dynamic Table | 約 1,000 件の全量テーブル。自動リフレッシュが最適 |
| TRANSACTIONS | MERGE（Stream 駆動） | 大量の差分データ。Stream で変更検知し MERGE で upsert |
| DAILY_MEMBER_SUMMARY | MERGE（Stream 駆動） | 98% が更新レコード。MERGE による差分更新が最適 |

---

#### 10_silver_members_dt.sql — MEMBERS の Dynamic Table

```sql
CREATE OR REPLACE DYNAMIC TABLE SILVER.MEMBERS
  TARGET_LAG = '1 hour'
  WAREHOUSE = RETAIL_WH
AS
SELECT
  RAW_DATA:"_id"::VARCHAR(100)          AS member_id,
  RAW_DATA:"email"::VARCHAR(5000)       AS email,
  RAW_DATA:"member_name"::VARCHAR(5000) AS member_name,
  -- ... 他のカラムも同様に VARIANT から型変換 ...
  CONVERT_TIMEZONE('UTC','Asia/Tokyo',
    RAW_DATA:"registered_at":"$date"::TIMESTAMP_NTZ
  )::TIMESTAMP_LTZ                      AS registered_at,
  -- ym / ymd パーティションキーも導出
FROM BRONZE.MEMBERS;
```

**Dynamic Table とは:**
- SELECT 文を定義するだけで、Snowflake が `TARGET_LAG`（この場合 1 時間）以内に自動でリフレッシュしてくれるテーブル
- ビューと違い、実体データを持つためクエリ性能が良い
- ビューと違い、リフレッシュタイミングは Snowflake に委ねる（厳密なリアルタイム性はない）

**VARIANT 型からの型変換パターン:**
```sql
-- 通常のフィールド
RAW_DATA:"field_name"::VARCHAR(100)

-- MongoDB の $date 型タイムスタンプ（ネストあり）
CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"field":"$date"::TIMESTAMP_NTZ)

-- パーティションキーの導出
TO_CHAR(registered_at, 'YYYYMM') AS ym
TO_CHAR(registered_at, 'YYYYMMDD') AS ymd
```

---

#### 11_silver_tables.sql — TRANSACTIONS / DAILY_MEMBER_SUMMARY テーブル

MERGE 先となる Silver テーブルの箱（空テーブル）を作成します。

**SILVER.TRANSACTIONS の主なカラム:**
- `_id_str VARCHAR(200) PRIMARY KEY` — MongoDB の Binary ObjectID（base64）
- `transaction_id`, `item_index`, `card_number`, `store_code` 等のビジネスカラム
- `transaction_date TIMESTAMP_LTZ` — UTC → JST 変換済み
- `ym CHAR(6)`, `ymd CHAR(8)` — パーティションキー

**SILVER.DAILY_MEMBER_SUMMARY の主なカラム:**
- `_id VARCHAR(100) PRIMARY KEY` — `{member_id}-{YYYY-MM-DD}` 形式
- `member_id`, `visit_count`, `purchase_amount`, `item_count` 等
- `has_purchased`, `has_returned` — BOOLEAN フラグ

> Snowflake の PRIMARY KEY は参考情報（enforced ではない）。ドキュメンテーション目的とオプティマイザのヒントとして定義。

---

#### 12_initial_merge.sql — 初回の MERGE 実行

Bronze の Stream から Silver テーブルへの初回データ投入を行います。

```sql
MERGE INTO SILVER.TRANSACTIONS AS tgt
USING (
  SELECT * FROM BRONZE.TRANSACTIONS_STREAM
  WHERE METADATA$ACTION = 'INSERT'
) AS src
ON tgt._id_str = src._id_str
WHEN MATCHED AND src.updated_at > tgt.updated_at THEN
  UPDATE SET ...   -- より新しいレコードで上書き
WHEN NOT MATCHED THEN
  INSERT (...) VALUES (...);  -- 新規レコードを挿入
```

**MERGE の仕組み:**
1. `ON` 句でソースとターゲットのキーを突合
2. `WHEN MATCHED` → 既存レコードが見つかった場合の処理（UPDATE）
3. `WHEN NOT MATCHED` → 新規レコードの処理（INSERT）
4. `src.updated_at > tgt.updated_at` で「より新しい場合のみ更新」を実現

**Stream からの読み取り:**
- `METADATA$ACTION = 'INSERT'` で INSERT 行のみを対象に
- Stream を DML（MERGE）内で読み取ると、自動的にオフセットが進む
- つまり、次回の MERGE では「前回以降の新しい変更」だけが処理される

**MongoDB 固有の型変換:**
```sql
-- Binary ObjectID（$binary → base64）
RAW_DATA:"_id":"$binary":"base64"::VARCHAR(200)

-- 64ビット整数（$numberLong）
RAW_DATA:"jan_code":"$numberLong"::BIGINT

-- 日付型（$date）
CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"field":"$date"::TIMESTAMP_NTZ)
```

---

### Phase N-O: 自動化 + Gold 層（13〜15）

#### 13_merge_tasks.sql — 自動実行タスクの作成

日次データロードと MERGE を自動実行する Task DAG（有向非巡回グラフ）を構築します。

```
BRONZE.DAILY_LOAD（毎日 01:00 JST に実行）
    │
    ├── SILVER.MERGE_TRANSACTIONS
    │   └── WHEN: TRANSACTIONS_STREAM にデータがあるとき
    │
    ├── SILVER.MERGE_DAILY_MEMBER_SUMMARY
    │   └── WHEN: DAILY_MEMBER_SUMMARY_STREAM にデータがあるとき
    │
    └── BRONZE.DATA_QUALITY_CHECK（15 で作成）
```

**DAILY_LOAD（親タスク）:**
```sql
CREATE OR REPLACE TASK BRONZE.DAILY_LOAD
  WAREHOUSE = RETAIL_WH
  SCHEDULE = 'USING CRON 0 1 * * * Asia/Tokyo'
AS
  CALL BRONZE.LOAD_ALL(DATEADD(day, -1, CURRENT_DATE()));
```
- cron 式 `0 1 * * *` = 毎日 01:00
- `DATEADD(day, -1, CURRENT_DATE())` で前日のデータをロード

**MERGE_TRANSACTIONS（子タスク）:**
```sql
CREATE OR REPLACE TASK BRONZE.MERGE_TRANSACTIONS
  WAREHOUSE = RETAIL_WH
  AFTER BRONZE.DAILY_LOAD
  WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.TRANSACTIONS_STREAM')
AS
  MERGE INTO SILVER.TRANSACTIONS AS tgt
  USING (...) ...;
```

**ポイント:**
- `AFTER` で親タスク完了後に実行される依存関係を定義
- `WHEN SYSTEM$STREAM_HAS_DATA(...)` で **Stream にデータがある場合のみ** 実行 → ウェアハウスコストを節約
- 子タスクは並列に実行される（MERGE_TRANSACTIONS と MERGE_DAILY_MEMBER_SUMMARY は同時に走る）

---

#### 14_gold_dynamic_tables.sql — Gold 層の集計テーブル

3 つの Dynamic Table を作成し、Silver 層のデータを自動集計します。

| Dynamic Table | ソース | 集計内容 |
|---|---|---|
| `GOLD.MONTHLY_ACTIVE_MEMBERS` | `SILVER.DAILY_MEMBER_SUMMARY` | 月別アクティブ会員数 |
| `GOLD.MONTHLY_SALES_SUMMARY` | `SILVER.TRANSACTIONS` | 月別売上集計（購入者数・取引件数・総売上） |
| `GOLD.MEMBER_PURCHASE_SUMMARY` | `SILVER.DAILY_MEMBER_SUMMARY` + `SILVER.MEMBERS` | 会員別の購入・返品日数 |

**全て `TARGET_LAG = '2 hours'`** — Silver 層（1 時間）より長めに設定。これにより:
- Silver の更新 → Gold の更新という連鎖リフレッシュが自動で行われる
- Bronze → Silver → Gold 全体の最大遅延は約 3 時間

```sql
-- 例: 月別アクティブ会員数
CREATE OR REPLACE DYNAMIC TABLE GOLD.MONTHLY_ACTIVE_MEMBERS
  TARGET_LAG = '2 hours'
  WAREHOUSE = RETAIL_WH
AS
SELECT
  ym                            AS 年月,
  COUNT(DISTINCT member_id)     AS アクティブ会員数
FROM SILVER.DAILY_MEMBER_SUMMARY
WHERE has_purchased = TRUE
GROUP BY ym;
```

---

#### 15_quality_task.sql — 品質チェックタスク

データロード後に自動で品質チェックを行うタスクを作成します。

```sql
CREATE OR REPLACE TASK BRONZE.DATA_QUALITY_CHECK
  WAREHOUSE = RETAIL_WH
  AFTER BRONZE.DAILY_LOAD
AS
BEGIN
  INSERT INTO MONITORING.QUALITY_LOG (TABLE_NAME, ROW_COUNT, STATUS, CHECKED_AT)
    SELECT 'BRONZE.MEMBERS', COUNT(*),
           CASE WHEN COUNT(*) = 0 THEN 'ALERT' ELSE 'OK' END,
           CURRENT_TIMESTAMP()
    FROM BRONZE.MEMBERS;
  -- TRANSACTIONS, DAILY_MEMBER_SUMMARY も同様
END;
```

- 各 Bronze テーブルの行数をチェック
- 行数が 0 → `ALERT`（パイプライン障害の可能性）
- 結果を `MONITORING.QUALITY_LOG` に記録

---

### Phase P-R: Task の運用（16〜17）

#### 16_task_resume.sql — 全タスクの有効化

```sql
-- 子タスクを先に RESUME
ALTER TASK BRONZE.MERGE_TRANSACTIONS RESUME;
ALTER TASK BRONZE.MERGE_DAILY_MEMBER_SUMMARY RESUME;
ALTER TASK BRONZE.DATA_QUALITY_CHECK RESUME;
-- 親タスクを最後に RESUME
ALTER TASK BRONZE.DAILY_LOAD RESUME;
```

**重要: 子タスク → 親タスクの順に有効化する。**
理由: 親タスクが先に有効化されると、スケジュール実行時に子タスクがまだ SUSPENDED で失敗するため。

---

#### 17_task_suspend.sql — 全タスクの無効化

```sql
-- 親タスクを先に SUSPEND
ALTER TASK BRONZE.DAILY_LOAD SUSPEND;
-- 子タスクを後に SUSPEND
ALTER TASK BRONZE.MERGE_TRANSACTIONS SUSPEND;
ALTER TASK BRONZE.MERGE_DAILY_MEMBER_SUMMARY SUSPEND;
ALTER TASK BRONZE.DATA_QUALITY_CHECK SUSPEND;
```

**重要: 親タスク → 子タスクの順に無効化する。**
理由: 子タスクを先に停止すると、稼働中の親タスクが子タスクを呼び出して失敗するため。

> RESUME と SUSPEND の順番は逆になる点に注意してください。

---

### Phase S: Snowflake 内 dbt 実行（18〜20、オプション）

SQL 13〜17 で構築した Task による MERGE パイプラインを、dbt に置き換えるオプション構成です。

---

#### 18_git_repository.sql — GitHub リポジトリとの接続

Snowflake から GitHub リポジトリにアクセスするための 3 つのオブジェクトを作成します。

```
Secret（認証情報）→ API Integration（ネットワーク設定）→ Git Repository（リポジトリ接続）
```

| 作成オブジェクト | 名前 | 説明 |
|---|---|---|
| Secret | `RETAIL_DWH.PUBLIC.GIT_SECRET` | GitHub PAT（Personal Access Token）を格納 |
| API Integration | `GIT_INTEGRATION` | 許可する GitHub URL のプレフィックスを定義 |
| Git Repository | `RETAIL_DWH.PUBLIC.DBT_REPO` | 実際のリポジトリへの接続 |

**事前準備:**
1. GitHub で Fine-grained PAT を生成（対象リポジトリのみにスコープ）
2. Private リポジトリの場合: `repo` スコープ / Public の場合: `public_repo` で OK

```sql
-- 1. GitHub 認証情報
CREATE OR REPLACE SECRET RETAIL_DWH.PUBLIC.GIT_SECRET
  TYPE = password
  USERNAME = 'your-github-username'
  PASSWORD = '<GitHub PAT>';

-- 2. API Integration
CREATE OR REPLACE API INTEGRATION GIT_INTEGRATION
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/your-github-username/')
  ALLOWED_AUTHENTICATION_SECRETS = (RETAIL_DWH.PUBLIC.GIT_SECRET)
  ENABLED = TRUE;

-- 3. Git リポジトリ
CREATE OR REPLACE GIT REPOSITORY RETAIL_DWH.PUBLIC.DBT_REPO
  API_INTEGRATION = GIT_INTEGRATION
  GIT_CREDENTIALS = RETAIL_DWH.PUBLIC.GIT_SECRET
  ORIGIN = 'https://github.com/your-github-username/snowflake_medallion_dbt.git';
```

---

#### 19_dbt_project.sql — dbt プロジェクトオブジェクトの作成

Snowflake 内で dbt を実行するためのオブジェクトを作成します。

**Network Rule（ネットワークルール）:**
dbt がパッケージをダウンロードするために必要な外部ホストへの通信を許可します。

```sql
CREATE OR REPLACE NETWORK RULE RETAIL_DWH.PUBLIC.DBT_PACKAGES_RULE
  MODE = EGRESS                    -- アウトバウンド通信
  TYPE = HOST_PORT
  VALUE_LIST = ('hub.getdbt.com', 'codeload.github.com');
```

> Snowflake はデフォルトで全ての外部通信をブロックするため、明示的な許可が必要。

**dbt Project オブジェクト:**
```sql
CREATE OR REPLACE DBT PROJECT RETAIL_DWH.PUBLIC.RETAIL_DBT_PROJECT
  FROM '@RETAIL_DWH.PUBLIC.DBT_REPO/branches/main/dbt_project'
  DEFAULT_TARGET = 'prod'
  EXTERNAL_ACCESS_INTEGRATIONS = (DBT_PACKAGES_ACCESS);
```

- `FROM`: Git リポジトリ内の dbt プロジェクトディレクトリを指定
- `DEFAULT_TARGET`: `profiles.yml` 内の `prod` ターゲットを使用
- 手動実行で動作確認: `EXECUTE DBT PROJECT ... ARGS = 'build --target prod';`

---

#### 20_dbt_task.sql — dbt タスクチェーンへの移行

既存の MERGE タスク（SQL 13 で作成）を停止し、代わりに dbt build を実行するタスクに切り替えます。

**変更前の Task DAG:**
```
DAILY_LOAD → MERGE_TRANSACTIONS
           → MERGE_DAILY_MEMBER_SUMMARY
           → DATA_QUALITY_CHECK
```

**変更後の Task DAG:**
```
DAILY_LOAD → DBT_BUILD（dbt build --target prod）
```

**手順:**
1. 既存タスクをすべて SUSPEND
2. `DBT_BUILD` タスクを作成（`AFTER BRONZE.DAILY_LOAD`）
3. `DBT_BUILD` → `DAILY_LOAD` の順に RESUME

```sql
CREATE OR REPLACE TASK RETAIL_DWH.PUBLIC.DBT_BUILD
  WAREHOUSE = RETAIL_WH
  AFTER BRONZE.DAILY_LOAD
AS
  EXECUTE DBT PROJECT RETAIL_DWH.PUBLIC.RETAIL_DBT_PROJECT
    ARGS = 'build --target prod';
```

**`dbt build` が行うこと:**
- 全 dbt モデル（Silver + Gold）の実行
- 全テストの実行
- スナップショットの実行
- すべて依存関係順に自動実行

**確認コマンド:**
```sql
-- Task DAG 全体の依存関係を表示
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS('BRONZE.DAILY_LOAD', RECURSIVE => TRUE));
```

---

## Snowflake 主要概念のまとめ

| 概念 | 説明 | 使用箇所 |
|---|---|---|
| **VARIANT 型** | JSON をそのまま格納する半構造化データ型。`RAW_DATA:"key"::TYPE` で抽出 | Bronze テーブル（07） |
| **Storage Integration** | 外部クラウドストレージへの認証設定。IAM ベースで安全 | GCS 接続（02-04） |
| **External Stage** | 外部ストレージのファイルを参照するポインタ | GCS → Bronze（04） |
| **COPY INTO** | 外部ファイルをテーブルにバルクロード | ストアドプロシージャ（08） |
| **Stream** | テーブルの変更（CDC）を追跡。DML で読み取ると消費される | Bronze → Silver（07, 12, 13） |
| **MERGE** | UPSERT（INSERT or UPDATE）を 1 SQL で実行 | Silver の差分更新（12, 13） |
| **Dynamic Table** | SELECT を定義するだけで自動リフレッシュされるテーブル | Silver MEMBERS（10）, Gold（14） |
| **Task** | CRON スケジューラ。AFTER で DAG を構成、WHEN で条件実行 | 自動化全般（13, 15-17, 20） |
| **Stored Procedure** | Snowflake Scripting（SQL ベース）の手続き型処理 | データロード（08） |
| **Git Integration** | GitHub リポジトリを Snowflake オブジェクトとして接続 | dbt 連携（18） |
| **dbt Project** | Snowflake 内で dbt を直接実行する機能 | dbt 連携（19, 20） |

---

## トラブルシューティング

| 問題 | 原因 | 対処 |
|---|---|---|
| `LIST @BRONZE.GCS_RAW_DATA` でエラー | IAM 未反映 / バケット名の typo | 数分待って再実行 / URL を確認 |
| `COPY INTO` で 0 行 | GCS のパス不一致 | `LIST @BRONZE.GCS_RAW_DATA/ym=.../ymd=.../` でファイル存在を確認 |
| MERGE で 0 行更新 | Stream が空（すでに消費済み） | Bronze テーブルに新規データをロードしてから再実行 |
| Task が動かない | SUSPENDED のまま | `SHOW TASKS` で STATE を確認し `ALTER TASK ... RESUME` |
| Dynamic Table が更新されない | ソーステーブルに変更なし | Bronze にデータを追加してから `TARGET_LAG` 時間待つ |
| dbt Project 作成でエラー | ネットワークルール未設定 | 19 の Network Rule と External Access Integration を先に作成 |
