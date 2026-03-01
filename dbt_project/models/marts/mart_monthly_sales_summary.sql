-- =============================================================================
-- Gold: 月別売上サマリ (Chapter 7 で学ぶ)
-- =============================================================================
-- 【このモデルの役割】
-- 月ごとの購入者数、取引件数、総売上を集計する。
-- 元の Dynamic Table GOLD.MONTHLY_SALES_SUMMARY (14_gold_dynamic_tables.sql) を置き換える。
--
-- 【ref() チェーンの実例】
-- bronze.transactions → stg_transactions → mart_monthly_sales_summary
-- dbt はこの依存チェーンを自動的に解決し、正しい順序でビルドする。
--
-- 【検証方法】
-- dbt run --select mart_monthly_sales_summary
-- 比較:
--   SELECT * FROM GOLD.MONTHLY_SALES_SUMMARY;
--   SELECT * FROM RETAIL_DWH.GOLD_DBT.MART_MONTHLY_SALES_SUMMARY;
-- =============================================================================

SELECT
    ym                                AS ym,
    COUNT(DISTINCT card_number)       AS buyer_count,
    COUNT(*)                          AS transaction_count,
    SUM(unit_price * quantity)        AS total_sales
FROM {{ ref('stg_transactions') }}
GROUP BY ym
