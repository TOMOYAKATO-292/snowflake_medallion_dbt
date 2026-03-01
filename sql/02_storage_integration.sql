-- =============================================================================
-- Phase B: Storage Integration 作成
-- =============================================================================
-- Snowflake から GCS にアクセスするための接続オブジェクトを作成する。
-- 作成後、STORAGE_GCP_SERVICE_ACCOUNT が自動生成される → Phase C で使用。
--
-- 【学べること】
-- - Storage Integration による外部ストレージ連携
-- - GCS / S3 / Azure Blob への安全なアクセス設定
-- =============================================================================

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION GCS_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'GCS'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('gcs://your-gcs-bucket-name/');

-- 作成確認（STORAGE_GCP_SERVICE_ACCOUNT を確認する）
DESCRIBE INTEGRATION GCS_INTEGRATION;
