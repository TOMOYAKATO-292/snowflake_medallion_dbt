-- =============================================================================
-- Phase I: 初期データロード
-- =============================================================================
-- LOAD_ALL プロシージャで初期データを Bronze 層にロード。
-- 日付は GCS 上のデータパーティションに合わせて指定する。
--
-- 【学べること】
-- - ストアドプロシージャの実行方法（CALL 文）
-- - ロード結果の確認方法
-- =============================================================================

USE DATABASE RETAIL_DWH;
USE WAREHOUSE RETAIL_WH;

-- 初期データロード（日付は環境に合わせて変更）
CALL BRONZE.LOAD_ALL('2026-02-15'::DATE);

-- 件数確認
SELECT 'BRONZE.MEMBERS' AS table_name, COUNT(*) AS row_count FROM BRONZE.MEMBERS
UNION ALL
SELECT 'BRONZE.TRANSACTIONS', COUNT(*) FROM BRONZE.TRANSACTIONS
UNION ALL
SELECT 'BRONZE.DAILY_MEMBER_SUMMARY', COUNT(*) FROM BRONZE.DAILY_MEMBER_SUMMARY;
