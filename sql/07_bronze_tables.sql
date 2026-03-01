-- =============================================================================
-- Phase G: Bronze テーブルとストリームの作成
-- =============================================================================
-- Bronze 層 = 生データを VARIANT 型でそのまま格納する層。
-- ストリーム = テーブルへの変更を追跡する CDC（Change Data Capture）メカニズム。
--
-- 【学べること】
-- - VARIANT 型: JSON をそのまま格納できる半構造化データ型
-- - Stream: テーブルの INSERT/UPDATE/DELETE を自動追跡する機能
--   → Silver 層への増分 MERGE のトリガーとして使用する
-- =============================================================================

USE DATABASE RETAIL_DWH;
USE SCHEMA BRONZE;

-- Bronze テーブル（RAW_DATA 列に JSON 全体を VARIANT 型で格納）
CREATE OR REPLACE TABLE BRONZE.MEMBERS (
    RAW_DATA   VARIANT,
    LOAD_DATE  DATE,
    LOADED_AT  TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE BRONZE.TRANSACTIONS (
    RAW_DATA   VARIANT,
    LOAD_DATE  DATE,
    LOADED_AT  TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE BRONZE.DAILY_MEMBER_SUMMARY (
    RAW_DATA   VARIANT,
    LOAD_DATE  DATE,
    LOADED_AT  TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ストリーム（各テーブルの変更を追跡）
CREATE OR REPLACE STREAM BRONZE.MEMBERS_STREAM ON TABLE BRONZE.MEMBERS;
CREATE OR REPLACE STREAM BRONZE.TRANSACTIONS_STREAM ON TABLE BRONZE.TRANSACTIONS;
CREATE OR REPLACE STREAM BRONZE.DAILY_MEMBER_SUMMARY_STREAM ON TABLE BRONZE.DAILY_MEMBER_SUMMARY;
