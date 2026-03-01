-- =============================================================================
-- Phase P: 品質チェックタスクの作成
-- =============================================================================
-- DAILY_LOAD の子タスク。各 Bronze テーブルの行数をカウントし品質チェック。
-- 行数0の場合は STATUS = 'ALERT' を記録（データ欠損の検知）。
--
-- 【学べること】
-- - データ品質監視の基本パターン
-- - 子タスクによるパイプライン後処理
-- - CASE 式による条件分岐
-- =============================================================================

USE DATABASE RETAIL_DWH;
USE WAREHOUSE RETAIL_WH;

CREATE OR REPLACE TASK BRONZE.DATA_QUALITY_CHECK
    WAREHOUSE = RETAIL_WH
    AFTER BRONZE.DAILY_LOAD
AS
BEGIN
    INSERT INTO MONITORING.QUALITY_LOG
        SELECT 'BRONZE.MEMBERS', COUNT(*),
               CASE WHEN COUNT(*) = 0 THEN 'ALERT' ELSE 'OK' END,
               CURRENT_TIMESTAMP()
        FROM BRONZE.MEMBERS;
    INSERT INTO MONITORING.QUALITY_LOG
        SELECT 'BRONZE.TRANSACTIONS', COUNT(*),
               CASE WHEN COUNT(*) = 0 THEN 'ALERT' ELSE 'OK' END,
               CURRENT_TIMESTAMP()
        FROM BRONZE.TRANSACTIONS;
    INSERT INTO MONITORING.QUALITY_LOG
        SELECT 'BRONZE.DAILY_MEMBER_SUMMARY', COUNT(*),
               CASE WHEN COUNT(*) = 0 THEN 'ALERT' ELSE 'OK' END,
               CURRENT_TIMESTAMP()
        FROM BRONZE.DAILY_MEMBER_SUMMARY;
END;
