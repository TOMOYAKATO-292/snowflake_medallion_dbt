-- =============================================================================
-- Phase 19: External Access 設定 + dbt Project オブジェクト作成
-- =============================================================================
-- dbt パッケージ（dbt_utils など）のダウンロードに外部ネットワークアクセスが必要。
-- その後、Git リポジトリから dbt プロジェクトを Snowflake オブジェクトとして登録する。
--
-- 前提: Phase 18（Git リポジトリ接続）が完了していること
--
-- 【学べること】
-- - NETWORK RULE: Snowflake からの外部通信を許可するルール
-- - EXTERNAL ACCESS INTEGRATION: ネットワークルールを束ねる統合オブジェクト
-- - DBT PROJECT: dbt プロジェクトを Snowflake ネイティブに実行するオブジェクト
-- =============================================================================

USE DATABASE RETAIL_DWH;
USE WAREHOUSE RETAIL_WH;

-- 1. dbt パッケージダウンロード用ネットワークルール
--    dbt deps 実行時に hub.getdbt.com と codeload.github.com への
--    アウトバウンド通信が必要
CREATE OR REPLACE NETWORK RULE RETAIL_DWH.PUBLIC.DBT_PACKAGES_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('hub.getdbt.com', 'codeload.github.com');

-- 2. External Access Integration（ACCOUNTADMIN ロール必要）
--    ネットワークルールを束ねて、dbt Project から参照できるようにする
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION DBT_PACKAGES_ACCESS
  ALLOWED_NETWORK_RULES = (RETAIL_DWH.PUBLIC.DBT_PACKAGES_RULE)
  ENABLED = TRUE;

-- 3. dbt Project オブジェクトを作成
--    リポジトリの dbt_project/ サブディレクトリを指定
--    DEFAULT_TARGET = 'prod' で Snowflake 内実行時のターゲットを設定
CREATE OR REPLACE DBT PROJECT RETAIL_DWH.PUBLIC.RETAIL_DBT_PROJECT
  FROM '@RETAIL_DWH.PUBLIC.DBT_REPO/branches/main/dbt_project'
  DEFAULT_TARGET = 'prod'
  EXTERNAL_ACCESS_INTEGRATIONS = (DBT_PACKAGES_ACCESS)
  COMMENT = 'Retail メダリオンアーキテクチャ dbt パイプライン';

-- 4. 確認
SHOW DBT PROJECTS IN SCHEMA RETAIL_DWH.PUBLIC;

-- 5. 手動実行で動作確認（任意）
-- EXECUTE DBT PROJECT RETAIL_DWH.PUBLIC.RETAIL_DBT_PROJECT
--   ARGS = 'build --target prod';
