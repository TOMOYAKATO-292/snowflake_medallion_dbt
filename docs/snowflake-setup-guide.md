# Snowflake 環境構築ガイド

## 全体像

### メダリオンアーキテクチャ

データレイクハウスの標準的なレイヤー設計パターン。

```
Bronze（生データ）→ Silver（変換済み）→ Gold（集計・分析用）
```

| レイヤー | 役割 | データ品質 |
|----------|------|-----------|
| Bronze | 外部ソースからの生データをそのまま格納 | 未加工 |
| Silver | 型変換・正規化・タイムゾーン変換等 | クレンジング済み |
| Gold | ビジネスロジックに基づく集計テーブル | 分析準備完了 |

### 処理フロー

```
GCS (JSONL.gz)
  ↓  COPY INTO
Bronze (VARIANT 型で生 JSON 格納)
  ↓  Dynamic Table / MERGE
Silver (型変換・正規化済み)
  ↓  Dynamic Table
Gold (集計テーブル)
```

---

## 主要な Snowflake 機能の解説

### Storage Integration

Snowflake が外部クラウドストレージ（GCS/S3/Azure Blob）にアクセスするための認証オブジェクト。Snowflake が内部的に GCP サービスアカウントを生成し、そのサービスアカウントに権限を付与する方式。

- クレデンシャルが SQL 文に露出しない
- IAM によるきめ細かいアクセス制御が可能

### VARIANT 型

Snowflake の半構造化データ型。JSON をそのまま格納でき、`RAW_DATA:"field_name"::TYPE` でフィールドを抽出できる。Bronze 層では全データを VARIANT 型の `RAW_DATA` カラムに格納し、Silver 層で型付きカラムに変換する。

### Dynamic Table

SELECT 文を定義するだけで、Snowflake が `TARGET_LAG` 以内に自動リフレッシュしてくれるテーブル。

- 全量リロードのテーブル（MEMBERS）に最適
- Gold 層の集計テーブルにも利用

### Stream（CDC）

テーブルへの INSERT/UPDATE/DELETE を自動追跡するオブジェクト。`SYSTEM$STREAM_HAS_DATA()` でデータの有無を確認でき、Task の `WHEN` 句で条件実行に利用する。

### Task

Snowflake のジョブスケジューラ。

- `SCHEDULE`: cron 式で定期実行
- `AFTER`: 親タスク完了後に実行（依存関係）
- `WHEN`: 条件を満たす場合のみ実行

RESUME/SUSPEND の順序に注意:
- 有効化: 子タスク → 親タスク
- 無効化: 親タスク → 子タスク

### MERGE

UPSERT（あれば UPDATE、なければ INSERT）を1つの SQL で実行。Bronze → Silver の差分更新に利用。`ON` 句で主キーを指定し、`WHEN MATCHED` / `WHEN NOT MATCHED` でそれぞれの処理を定義する。

---

## dbt 機能の解説

### source() と ref()

- `source()`: dbt 管理外のテーブル（Bronze）を参照
- `ref()`: dbt が管理するモデルを参照（依存関係を自動解決）

### materialization

| 種類 | 動作 | 用途 |
|------|------|------|
| view | CREATE VIEW | 少量データの Silver 層 |
| table | CREATE TABLE AS SELECT | Gold 層の集計テーブル |
| incremental | 初回は table、2 回目以降は MERGE | 大量データの Silver 層 |

### macro

Jinja テンプレートで再利用可能な SQL の部品を定義。タイムゾーン変換やパーティションキー生成など、繰り返し使うパターンをマクロ化することで保守性が向上する。

### snapshot (SCD Type 2)

レコードの変更履歴を自動記録する仕組み。`dbt_valid_from` / `dbt_valid_to` カラムで各バージョンの有効期間を管理する。

---

## セットアップ手順

1. Snowflake Trial アカウントを作成
2. GCS バケットを作成し、サンプルデータをアップロード
3. `.env.example` を `.env` にコピーし、認証情報を記入
4. `sql/` 配下のファイルを Snowsight で番号順に実行
5. dbt プロジェクトを実行する場合は `dbt_project/` で環境構築
