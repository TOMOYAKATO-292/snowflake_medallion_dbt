-- =============================================================================
-- Phase O: Gold Dynamic Tables の作成
-- =============================================================================
-- Gold 層 = ビジネスロジックに基づいた集計テーブル（メダリオンアーキテクチャの最上層）。
-- すべて Dynamic Table (TARGET_LAG = 2 hours) で自動リフレッシュ。
--
-- 【学べること】
-- - Gold 層の設計パターン: ビジネスの問いに直接答えるテーブルを作る
-- - Dynamic Table の連鎖: Silver → Gold の自動更新チェーン
-- - 集計関数: COUNT(DISTINCT), SUM, CASE WHEN の活用
-- - JOIN: 複数の Silver テーブルを結合してビジネスインサイトを作る
-- =============================================================================

-- 1. 月別アクティブ会員数
CREATE OR REPLACE DYNAMIC TABLE GOLD.MONTHLY_ACTIVE_MEMBERS
    TARGET_LAG = '2 hours'
    WAREHOUSE = RETAIL_WH
AS
SELECT
    ym                                AS "年月",
    COUNT(DISTINCT member_id)         AS "アクティブ会員数"
FROM SILVER.DAILY_MEMBER_SUMMARY
WHERE has_purchased = TRUE
GROUP BY ym;

-- 2. 月別売上サマリ
CREATE OR REPLACE DYNAMIC TABLE GOLD.MONTHLY_SALES_SUMMARY
    TARGET_LAG = '2 hours'
    WAREHOUSE = RETAIL_WH
AS
SELECT
    ym                                AS "年月",
    COUNT(DISTINCT card_number)       AS "購入者数",
    COUNT(*)                          AS "取引件数",
    SUM(unit_price * quantity)        AS "総売上"
FROM SILVER.TRANSACTIONS
GROUP BY ym;

-- 3. 会員別購買サマリ
CREATE OR REPLACE DYNAMIC TABLE GOLD.MEMBER_PURCHASE_SUMMARY
    TARGET_LAG = '2 hours'
    WAREHOUSE = RETAIL_WH
AS
SELECT
    dms.member_id,
    m.member_name,
    COUNT(*)                                                  AS "集計日数",
    SUM(CASE WHEN dms.has_purchased THEN 1 ELSE 0 END)       AS "購入日数",
    SUM(CASE WHEN dms.has_returned THEN 1 ELSE 0 END)        AS "返品日数"
FROM SILVER.DAILY_MEMBER_SUMMARY dms
JOIN SILVER.MEMBERS m
    ON dms.member_id = CAST(m.member_id AS INTEGER)
GROUP BY dms.member_id, m.member_name;

-- 手動リフレッシュ
ALTER DYNAMIC TABLE GOLD.MONTHLY_ACTIVE_MEMBERS REFRESH;
ALTER DYNAMIC TABLE GOLD.MONTHLY_SALES_SUMMARY REFRESH;
ALTER DYNAMIC TABLE GOLD.MEMBER_PURCHASE_SUMMARY REFRESH;

-- 件数確認
SELECT 'GOLD.MONTHLY_ACTIVE_MEMBERS' AS table_name, COUNT(*) AS row_count FROM GOLD.MONTHLY_ACTIVE_MEMBERS
UNION ALL
SELECT 'GOLD.MONTHLY_SALES_SUMMARY', COUNT(*) FROM GOLD.MONTHLY_SALES_SUMMARY
UNION ALL
SELECT 'GOLD.MEMBER_PURCHASE_SUMMARY', COUNT(*) FROM GOLD.MEMBER_PURCHASE_SUMMARY;
