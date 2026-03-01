-- =============================================================================
-- Phase 18: Git リポジトリを Snowflake に接続
-- =============================================================================
-- GitHub リポジトリと Snowflake を連携し、dbt プロジェクトのソースコードを
-- Snowflake 内から参照できるようにする。
--
-- 前提: GitHub PAT（Personal Access Token）を事前に発行しておくこと
--       スコープ: repo（プライベートリポジトリの場合）または public_repo
--       推奨: Fine-grained PAT で対象リポジトリのみに限定
--
-- 【学べること】
-- - Snowflake の Git Integration 機能
-- - SECRET / API INTEGRATION / GIT REPOSITORY オブジェクトの関係
-- - GitHub Fine-grained PAT の使い方
-- =============================================================================

USE DATABASE RETAIL_DWH;
USE WAREHOUSE RETAIL_WH;

-- 1. GitHub 認証用シークレット
--    PAT をパスワードとして保存する Snowflake オブジェクト
CREATE OR REPLACE SECRET RETAIL_DWH.PUBLIC.GIT_SECRET
  TYPE = password
  USERNAME = 'your-github-username'
  PASSWORD = '<GitHub PAT をここに入力>';

-- 2. API Integration（ACCOUNTADMIN ロール必要）
--    Snowflake が外部 Git ホスティングサービスと通信するための設定
CREATE OR REPLACE API INTEGRATION GIT_INTEGRATION
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/your-github-username/')
  ALLOWED_AUTHENTICATION_SECRETS = (RETAIL_DWH.PUBLIC.GIT_SECRET)
  ENABLED = TRUE;

-- 3. Git Repository オブジェクト
--    GitHub リポジトリを Snowflake 内のオブジェクトとして登録
CREATE OR REPLACE GIT REPOSITORY RETAIL_DWH.PUBLIC.DBT_REPO
  API_INTEGRATION = GIT_INTEGRATION
  GIT_CREDENTIALS = RETAIL_DWH.PUBLIC.GIT_SECRET
  ORIGIN = 'https://github.com/your-github-username/snowflake_medallion_dbt.git';

-- 4. 接続確認
--    FETCH でリポジトリの最新情報を取得し、ブランチ一覧を表示
ALTER GIT REPOSITORY RETAIL_DWH.PUBLIC.DBT_REPO FETCH;
SHOW GIT BRANCHES IN RETAIL_DWH.PUBLIC.DBT_REPO;
-- → main ブランチが表示されれば接続成功
