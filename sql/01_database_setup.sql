-- =============================================================================
-- Phase A: Database / Schema / Warehouse 作成
-- =============================================================================
-- メダリオンアーキテクチャの基盤となるオブジェクトを構築する。
--
-- 【メダリオンアーキテクチャとは】
-- データレイクハウスの設計パターン。データを3層に分けて段階的に品質を高める:
--   Bronze（生データ） → Silver（クレンジング済み） → Gold（集計・分析用）
--
-- 【学べること】
-- - Snowflake の Database / Schema / Warehouse の基本構造
-- - AUTO_SUSPEND / AUTO_RESUME によるコスト最適化
-- =============================================================================

CREATE DATABASE IF NOT EXISTS RETAIL_DWH;
USE DATABASE RETAIL_DWH;

-- メダリオンアーキテクチャ 3層 + 監視用
CREATE SCHEMA IF NOT EXISTS BRONZE;       -- 生データ格納層
CREATE SCHEMA IF NOT EXISTS SILVER;       -- 変換・クレンジング層
CREATE SCHEMA IF NOT EXISTS GOLD;         -- 集計・分析層
CREATE SCHEMA IF NOT EXISTS MONITORING;   -- 運用監視用

-- 計算リソース（最小サイズ、60秒無操作で自動停止）
CREATE WAREHOUSE IF NOT EXISTS RETAIL_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

USE WAREHOUSE RETAIL_WH;
