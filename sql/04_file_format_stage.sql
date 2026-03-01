-- =============================================================================
-- Phase D: ファイルフォーマットと外部ステージの作成
-- =============================================================================
-- JSONL.gz 形式のファイルを GCS から読み込むための設定。
--
-- 【学べること】
-- - FILE FORMAT: データの読み込み形式を定義するオブジェクト
-- - STAGE: 外部ストレージ上のデータを参照するオブジェクト
-- - Storage Integration との連携
-- =============================================================================

USE DATABASE RETAIL_DWH;
USE SCHEMA BRONZE;

-- JSONL.gz を読むためのフォーマット定義
CREATE OR REPLACE FILE FORMAT JSONL_GZ
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = FALSE
    COMPRESSION = 'GZIP';

-- GCS 上のデータを参照する外部ステージ
CREATE OR REPLACE STAGE GCS_RAW_DATA
    STORAGE_INTEGRATION = GCS_INTEGRATION
    URL = 'gcs://your-gcs-bucket-name/raw-data/'
    FILE_FORMAT = JSONL_GZ;
