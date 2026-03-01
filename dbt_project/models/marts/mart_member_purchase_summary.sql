-- =============================================================================
-- Gold: 会員別購買サマリ (Chapter 7 で学ぶ)
-- =============================================================================
-- 【このモデルの役割】
-- 会員ごとの集計日数、購入日数、返品日数を集計する。
-- 元の Dynamic Table GOLD.MEMBER_PURCHASE_SUMMARY (14_gold_dynamic_tables.sql) を置き換える。
--
-- 【JOIN と ref() の組み合わせ】
-- このモデルでは2つの staging モデルを JOIN している:
-- - stg_daily_member_summary: 日次集計データ（購入/返品フラグ）
-- - stg_members: 会員情報（会員名）
--
-- ref() を使うことで、dbt は両方のモデルが先にビルドされることを保証する。
-- リネージグラフでは2つの矢印がこのモデルに向かって表示される。
--
-- 【JOIN 条件の注意点】
-- daily_member_summary の member_id は INTEGER、members の member_id は VARCHAR のため、
-- CAST(m.member_id AS INTEGER) で型を合わせている。
--
-- 【検証方法】
-- dbt run --select mart_member_purchase_summary
-- 比較:
--   SELECT * FROM GOLD.MEMBER_PURCHASE_SUMMARY LIMIT 10;
--   SELECT * FROM RETAIL_DWH.GOLD_DBT.MART_MEMBER_PURCHASE_SUMMARY LIMIT 10;
-- =============================================================================

SELECT
    dms.member_id,
    m.member_name,
    COUNT(*)                                                  AS total_days,
    SUM(CASE WHEN dms.has_purchased THEN 1 ELSE 0 END)       AS purchase_days,
    SUM(CASE WHEN dms.has_returned THEN 1 ELSE 0 END)        AS return_days
FROM {{ ref('stg_daily_member_summary') }} dms
JOIN {{ ref('stg_members') }} m
    ON dms.member_id = CAST(m.member_id AS INTEGER)
GROUP BY dms.member_id, m.member_name
