-- =============================================================================
-- Phase Q: 全タスクの RESUME（有効化）
-- =============================================================================
-- タスクは作成時にデフォルトで SUSPENDED（停止中）。
-- 重要: 子タスクを先に RESUME してから、ルートタスクを RESUME する。
--
-- 【学べること】
-- - Task のライフサイクル管理（SUSPENDED → STARTED）
-- - 親子タスクの有効化順序（子 → 親）
-- =============================================================================

USE DATABASE RETAIL_DWH;
USE WAREHOUSE RETAIL_WH;

-- 子タスクを先に RESUME
ALTER TASK BRONZE.MERGE_TRANSACTIONS RESUME;
ALTER TASK BRONZE.MERGE_DAILY_MEMBER_SUMMARY RESUME;
ALTER TASK BRONZE.DATA_QUALITY_CHECK RESUME;

-- ルートタスクを最後に RESUME
ALTER TASK BRONZE.DAILY_LOAD RESUME;
