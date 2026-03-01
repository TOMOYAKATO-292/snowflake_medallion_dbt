-- =============================================================================
-- Phase R: 全タスクの SUSPEND（無効化）
-- =============================================================================
-- 重要: ルートタスクを先に SUSPEND してから、子タスクを SUSPEND する（RESUME と逆順）。
--
-- 【学べること】
-- - 親子タスクの無効化順序（親 → 子）
-- - RESUME と SUSPEND で順序が逆になる理由:
--   親が動いている状態で子を止めると、親が子を起動しようとしてエラーになる
-- =============================================================================

USE DATABASE RETAIL_DWH;
USE WAREHOUSE RETAIL_WH;

-- ルートタスクを先に SUSPEND
ALTER TASK BRONZE.DAILY_LOAD SUSPEND;

-- 子タスクを SUSPEND
ALTER TASK BRONZE.MERGE_TRANSACTIONS SUSPEND;
ALTER TASK BRONZE.MERGE_DAILY_MEMBER_SUMMARY SUSPEND;
ALTER TASK BRONZE.DATA_QUALITY_CHECK SUSPEND;
