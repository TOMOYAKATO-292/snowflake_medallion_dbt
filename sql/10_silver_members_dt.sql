-- =============================================================================
-- Phase K: Silver Dynamic Table (MEMBERS) の作成
-- =============================================================================
-- Dynamic Table = ソーステーブルが更新されると TARGET_LAG 以内に自動リフレッシュ。
-- RAW_DATA (VARIANT) から各フィールドを抽出・型変換し、UTC → JST 変換を行う。
--
-- 【学べること】
-- - Dynamic Table: 宣言的にクエリを定義すると、Snowflake が自動で最新化する
-- - VARIANT 型からの JSON フィールド抽出（:: による型キャスト）
-- - CONVERT_TIMEZONE による MongoDB $date 形式のタイムスタンプ変換
-- - TARGET_LAG: 許容するデータ鮮度の遅延（ここでは1時間）
--
-- 【MEMBERS を Dynamic Table にする理由】
-- 毎日全件リロード（約1,000行）なので、Stream + MERGE は不要。
-- Dynamic Table なら SELECT 文を書くだけで自動リフレッシュされる。
-- =============================================================================

USE DATABASE RETAIL_DWH;
USE WAREHOUSE RETAIL_WH;

CREATE OR REPLACE DYNAMIC TABLE SILVER.MEMBERS
    TARGET_LAG = '1 hour'
    WAREHOUSE = RETAIL_WH
AS
SELECT
    -- 基本情報
    RAW_DATA:"_id"::VARCHAR(100)                           AS member_id,
    RAW_DATA:"email"::VARCHAR(5000)                        AS email,
    RAW_DATA:"member_name"::VARCHAR(5000)                  AS member_name,
    RAW_DATA:"gender"::VARCHAR(10)                         AS gender,
    RAW_DATA:"birth_year"::INTEGER                         AS birth_year,
    RAW_DATA:"zip_code"::VARCHAR(20)                       AS zip_code,

    -- 店舗・カード情報
    RAW_DATA:"home_store_id"::INTEGER                      AS home_store_id,
    RAW_DATA:"card_number"::VARCHAR(5000)                  AS card_number,

    -- ステータス
    RAW_DATA:"is_active"::BOOLEAN                          AS is_active,
    RAW_DATA:"member_rank"::VARCHAR(20)                    AS member_rank,
    RAW_DATA:"total_points"::INTEGER                       AS total_points,

    -- タイムスタンプ（MongoDB の $date フォーマットから UTC → JST 変換）
    CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"registered_at":"$date"::TIMESTAMP_NTZ)::TIMESTAMP_LTZ   AS registered_at,
    CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"last_visit_at":"$date"::TIMESTAMP_NTZ)::TIMESTAMP_LTZ   AS last_visit_at,
    CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"updated_at":"$date"::TIMESTAMP_NTZ)::TIMESTAMP_LTZ      AS updated_at,

    -- パーティションキー（登録日ベース）
    TO_CHAR(CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"registered_at":"$date"::TIMESTAMP_NTZ), 'YYYYMM')    AS ym,
    TO_CHAR(CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"registered_at":"$date"::TIMESTAMP_NTZ), 'YYYYMMDD')  AS ymd,

    -- メタデータ
    LOAD_DATE
FROM BRONZE.MEMBERS;

-- 手動リフレッシュ（初回データ反映）
ALTER DYNAMIC TABLE SILVER.MEMBERS REFRESH;

-- 件数確認
SELECT COUNT(*) FROM SILVER.MEMBERS;
