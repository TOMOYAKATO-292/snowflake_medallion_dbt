# 実装ドキュメント

## 概要

GCS → Snowflake のメダリオンアーキテクチャ（Bronze / Silver / Gold）ETL パイプラインの実装詳細。

```
GCS バケット（JSONL.gz）
    ↓ External Stage
Snowflake
    ├─ Bronze … Task + COPY INTO で GCS からロード
    │    ↓ Stream が変更検知
    ├─ Silver … 全量: Dynamic Table / 日次: Task + MERGE
    │    ↓
    └─ Gold  … Dynamic Table（自動リフレッシュ）
```

---

## SQL スクリプト実行順序

| Phase | ファイル | 内容 |
|-------|---------|------|
| A | 01_database_setup.sql | Database `RETAIL_DWH`, Schema x4, Warehouse `RETAIL_WH` |
| B | 02_storage_integration.sql | Storage Integration `GCS_INTEGRATION` |
| C | 03_iam_setup.sql | IAM 連携（GCS → Snowflake サービスアカウント権限付与） |
| D | 04_file_format_stage.sql | File Format `JSONL_GZ` + External Stage `GCS_RAW_DATA` |
| E | 05_stage_verify.sql | `LIST @stage` 接続確認 |
| F | 06_monitoring_tables.sql | Monitoring テーブル（`LOAD_LOG`, `QUALITY_LOG`） |
| G | 07_bronze_tables.sql | Bronze テーブル x3 + Stream x3 |
| H | 08_stored_procedures.sql | Stored Procedure（`LOAD_COLLECTION`, `LOAD_ALL`） |
| I | 09_initial_load.sql | 初期データロード実行 |
| K | 10_silver_members_dt.sql | Silver Dynamic Table `MEMBERS`（TARGET_LAG 1 時間） |
| L | 11_silver_tables.sql | Silver テーブル `TRANSACTIONS`, `DAILY_MEMBER_SUMMARY` |
| M | 12_initial_merge.sql | 初回 MERGE（Stream 消費 → Silver 投入） |
| N | 13_merge_tasks.sql | MERGE Task x2 + DAILY_LOAD Task（Stream 駆動） |
| O | 14_gold_dynamic_tables.sql | Gold Dynamic Table x3（TARGET_LAG 2 時間） |
| P | 15_quality_task.sql | Data Quality Check Task |
| Q | 16_task_resume.sql | Task RESUME（子 → 親の順） |
| R | 17_task_suspend.sql | Task SUSPEND（親 → 子の順） |
| 18 | 18_git_repository.sql | Git リポジトリ接続 |
| 19 | 19_dbt_project.sql | dbt Project オブジェクト作成 |
| 20 | 20_dbt_task.sql | dbt Task チェーン構築 |

---

## Snowflake オブジェクト構成

```
RETAIL_DWH (Database)
│
├── BRONZE (Schema)
│   ├── [Stage]      GCS_RAW_DATA
│   ├── [FileFormat] JSONL_GZ
│   ├── [Table]      MEMBERS / TRANSACTIONS / DAILY_MEMBER_SUMMARY
│   ├── [Stream]     MEMBERS_STREAM / TRANSACTIONS_STREAM / DAILY_MEMBER_SUMMARY_STREAM
│   ├── [Procedure]  LOAD_COLLECTION / LOAD_ALL
│   └── [Task]       DAILY_LOAD / MERGE_TRANSACTIONS / MERGE_DAILY_MEMBER_SUMMARY / DATA_QUALITY_CHECK
│
├── SILVER (Schema)
│   ├── [Dynamic Table] MEMBERS                       ← Bronze 参照、自動更新
│   ├── [Table]         TRANSACTIONS                  ← MERGE 先
│   └── [Table]         DAILY_MEMBER_SUMMARY          ← MERGE 先
│
├── GOLD (Schema)
│   ├── [Dynamic Table] MONTHLY_ACTIVE_MEMBERS
│   ├── [Dynamic Table] MONTHLY_SALES_SUMMARY
│   └── [Dynamic Table] MEMBER_PURCHASE_SUMMARY
│
├── MONITORING (Schema)
│   └── [Table]     LOAD_LOG / QUALITY_LOG
│
└── PUBLIC (Schema)
    ├── [Secret]     GIT_SECRET
    ├── [Git Repo]   DBT_REPO
    ├── [dbt Project] RETAIL_DBT_PROJECT
    └── [Task]       DBT_BUILD
```

---

## Task 依存関係

```
BRONZE.DAILY_LOAD（CRON 毎日 01:00 JST）
    │
    ├── BRONZE.MERGE_TRANSACTIONS          (AFTER, Stream 駆動)
    ├── BRONZE.MERGE_DAILY_MEMBER_SUMMARY  (AFTER, Stream 駆動)
    ├── BRONZE.DATA_QUALITY_CHECK          (AFTER)
    │
    └── SILVER.MEMBERS は Dynamic Table → 自動リフレッシュ
         └── GOLD.* も Dynamic Table → 自動リフレッシュ
```

dbt Task チェーン構築後（Phase 20）:

```
BRONZE.DAILY_LOAD（CRON 毎日 01:00 JST）
    └── PUBLIC.DBT_BUILD  ← dbt build --target prod
```

---

## 検証データ

データソースは MongoDB の mongoexport 形式（JSONL.gz）を GCS に配置。

| コレクション | 方式 | 規模 | 検証ポイント |
|------------|------|------|------------|
| members | 全量（Full） | 1,000 件 | Dynamic Table 自動リフレッシュ |
| transactions | 日次（Daily） | 初期 10 万 + 差分 1 万 | Stream + MERGE（1.7% 更新） |
| daily_member_summary | 日次（Daily） | 初期 35 万 + 差分 1 万 | Stream + MERGE（98% 更新） |
