-- =============================================================================
-- Gold: 月別アクティブ会員数 (Chapter 4, 7 で学ぶ)
-- =============================================================================
-- 【このモデルの役割】
-- 月ごとの購入があったユニーク会員数を集計する。
-- 元の Dynamic Table GOLD.MONTHLY_ACTIVE_MEMBERS (14_gold_dynamic_tables.sql) を置き換える。
--
-- 【ref() 関数の使い方】
-- ref('stg_daily_member_summary') と書くと:
-- 1. stg_daily_member_summary モデルの出力テーブルを自動参照
-- 2. dbt が依存関係を認識し、stg_daily_member_summary を先にビルドしてくれる
-- 3. リネージグラフに依存関係が表示される
--
-- これが source() との違い:
-- - source(): dbt の管理外のテーブル（Bronze）を参照
-- - ref():    dbt が管理するモデルを参照
--
-- 【materialization: table】
-- dbt_project.yml で marts のデフォルトを table に設定済み。
-- 集計結果は少量なので、毎回フルリビルドしても高速。
--
-- 【検証方法】
-- dbt run --select mart_monthly_active_members
-- 既存テーブルと比較:
--   SELECT * FROM GOLD.MONTHLY_ACTIVE_MEMBERS ORDER BY "年月";
--   SELECT * FROM RETAIL_DWH.GOLD_DBT.MART_MONTHLY_ACTIVE_MEMBERS ORDER BY ym;
-- =============================================================================

SELECT
    ym                                AS ym,
    COUNT(DISTINCT member_id)         AS active_member_count
FROM {{ ref('stg_daily_member_summary') }}
WHERE has_purchased = TRUE
GROUP BY ym
