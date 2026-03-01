# 次のステップ

## パイプライン構築後にやること

### 1. 差分ロードの検証

```sql
-- デルタデータをロード
CALL BRONZE.LOAD_ALL('2026-02-16'::DATE);

-- Stream にデータがあることを確認
SELECT SYSTEM$STREAM_HAS_DATA('BRONZE.TRANSACTIONS_STREAM');
SELECT SYSTEM$STREAM_HAS_DATA('BRONZE.DAILY_MEMBER_SUMMARY_STREAM');

-- MERGE Task を手動実行
EXECUTE TASK BRONZE.MERGE_TRANSACTIONS;
EXECUTE TASK BRONZE.MERGE_DAILY_MEMBER_SUMMARY;

-- Silver 件数確認
SELECT 'SILVER.MEMBERS' AS table_name, COUNT(*) FROM SILVER.MEMBERS
UNION ALL SELECT 'SILVER.TRANSACTIONS', COUNT(*) FROM SILVER.TRANSACTIONS
UNION ALL SELECT 'SILVER.DAILY_MEMBER_SUMMARY', COUNT(*) FROM SILVER.DAILY_MEMBER_SUMMARY;
```

### 2. dbt による変換（SQL パイプラインの代替）

```bash
cd dbt_project
dbt run      # 全モデル実行
dbt test     # テスト実行
dbt docs generate && dbt docs serve  # ドキュメント確認
```

### 3. Snowsight でダッシュボード作成

Gold スキーマの Dynamic Table からチャートを作成:
- `GOLD.MONTHLY_ACTIVE_MEMBERS` → 月別アクティブ会員数
- `GOLD.MONTHLY_SALES_SUMMARY` → 月別売上推移
- `GOLD.MEMBER_PURCHASE_SUMMARY` → 会員別購買分析

### 4. Snowflake 内 dbt 実行（SQL 18〜20）

Git リポジトリ接続 → dbt Project 作成 → Task チェーン構築で、
Snowflake の Task から直接 dbt を実行できるようになる。

---

## 発展的なトピック

- **コスト最適化**: Warehouse サイズの調整、AUTO_SUSPEND の短縮
- **データ品質強化**: dbt の freshness チェック、カスタムテストの追加
- **本番運用**: RBAC（ロールベースアクセス制御）の設定、ACCOUNTADMIN 以外のロール利用
- **スケール検証**: データ量を増やしてパフォーマンスを計測

---

## クリーンアップ

```sql
-- Snowsight で実行
DROP DATABASE RETAIL_DWH;
DROP WAREHOUSE RETAIL_WH;
DROP STORAGE INTEGRATION GCS_INTEGRATION;
```

```bash
# GCS
gcloud storage rm -r gs://your-gcs-bucket-name
```

Snowflake Trial は 30 日で自動失効するため、放置でも問題なし。
