-- =============================================================================
-- Phase F: 監視テーブルの作成
-- =============================================================================
-- データロード履歴と品質チェック結果を記録するテーブル。
-- 本番運用ではこの監視データをダッシュボードで可視化する。
--
-- 【学べること】
-- - ETL パイプラインの運用監視の基本設計
-- - ロードログ / 品質ログの分離管理
-- =============================================================================

USE DATABASE RETAIL_DWH;
USE SCHEMA MONITORING;

-- データロードの実行履歴
CREATE TABLE IF NOT EXISTS MONITORING.LOAD_LOG (
    COLLECTION_NAME  VARCHAR,
    LOAD_TYPE        VARCHAR,
    LOAD_DATE        DATE,
    STATUS           VARCHAR,
    EXECUTED_AT      TIMESTAMP_LTZ
);

-- データ品質チェックの結果
CREATE TABLE IF NOT EXISTS MONITORING.QUALITY_LOG (
    TABLE_NAME   VARCHAR,
    ROW_COUNT    INTEGER,
    STATUS       VARCHAR,
    CHECKED_AT   TIMESTAMP_LTZ
);
