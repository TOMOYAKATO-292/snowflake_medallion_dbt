-- =============================================================================
-- Phase 20: dbt Task チェーンの構築
-- =============================================================================
-- 既存の MERGE / Quality Check タスクを dbt が代替するため無効化し、
-- DAILY_LOAD の子タスクとして dbt build を実行する Task を作成する。
--
-- 前提: Phase 19（dbt Project 作成）が完了していること
--       EXECUTE DBT PROJECT の手動実行で成功を確認済みであること
--
-- Task 構成（変更後）:
--   BRONZE.DAILY_LOAD (毎日 01:00 JST)  ← 既存のまま
--     └── RETAIL_DWH.PUBLIC.DBT_BUILD   ← 新規: dbt build を実行
--
-- 既存の子タスク（MERGE_TRANSACTIONS, MERGE_DAILY_MEMBER_SUMMARY, DATA_QUALITY_CHECK）は
-- dbt の incremental モデルと dbt test が代替するため無効化する。
--
-- 【学べること】
-- - dbt build と Snowflake Task の統合
-- - タスクチェーンの組み替え（既存タスクの無効化 → 新タスクの追加）
-- - EXECUTE DBT PROJECT 構文
-- =============================================================================

USE DATABASE RETAIL_DWH;
USE WAREHOUSE RETAIL_WH;

-- ============================================================
-- 1. 既存タスクを無効化（ルート → 子の順）
-- ============================================================
ALTER TASK BRONZE.DAILY_LOAD SUSPEND;

-- dbt が代替する子タスクを無効化
ALTER TASK BRONZE.MERGE_TRANSACTIONS SUSPEND;
ALTER TASK BRONZE.MERGE_DAILY_MEMBER_SUMMARY SUSPEND;
ALTER TASK BRONZE.DATA_QUALITY_CHECK SUSPEND;

-- ============================================================
-- 2. dbt build タスクを作成（DAILY_LOAD の子タスク）
-- ============================================================
-- 重要:
--   - サーバーレスタスクは dbt Project では使用不可
--     → 必ず WAREHOUSE を指定する（user-managed）
--   - Task は dbt Project と同じスキーマに作成する必要がある
--     → RETAIL_DWH.PUBLIC に配置
CREATE OR REPLACE TASK RETAIL_DWH.PUBLIC.DBT_BUILD
  WAREHOUSE = RETAIL_WH
  AFTER BRONZE.DAILY_LOAD
AS
  EXECUTE DBT PROJECT RETAIL_DWH.PUBLIC.RETAIL_DBT_PROJECT
    ARGS = 'build --target prod';

-- ============================================================
-- 3. 有効化（子 → 親の順）
-- ============================================================
ALTER TASK RETAIL_DWH.PUBLIC.DBT_BUILD RESUME;
ALTER TASK BRONZE.DAILY_LOAD RESUME;

-- ============================================================
-- 4. 確認
-- ============================================================
SHOW TASKS IN SCHEMA RETAIL_DWH.PUBLIC;
SHOW TASKS IN SCHEMA BRONZE;

-- タスク依存関係の確認
SELECT *
  FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
    TASK_NAME => 'BRONZE.DAILY_LOAD',
    RECURSIVE => TRUE
  ));
