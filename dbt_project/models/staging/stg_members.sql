-- =============================================================================
-- Silver Members モデル (Chapter 3 で学ぶ)
-- =============================================================================
-- 【このモデルの役割】
-- Bronze.MEMBERS の RAW_DATA (VARIANT) から各フィールドを抽出・型変換する。
-- 元の Dynamic Table SILVER.MEMBERS (10_silver_members_dt.sql) と同じロジック。
--
-- 【materialization: view】
-- dbt_project.yml で staging のデフォルトを view に設定済み。
-- view = CREATE VIEW を実行。テーブルは作らない。
-- MEMBERS は約1,000行と少量なので、毎回クエリしても十分高速。
-- Dynamic Table (1時間ラグ) と比べると、view は常に最新データを返す利点がある。
--
-- 【source() 関数の使い方】
-- source('bronze', 'members') と書くと:
-- 1. models/sources.yml で定義した RETAIL_DWH.BRONZE.MEMBERS テーブルを参照
-- 2. dbt docs のリネージグラフに依存関係が表示される
-- 3. テーブル名が変わっても sources.yml を1箇所変えるだけで OK
--
-- 【検証方法】
-- dbt run --select stg_members
-- 実行後、Snowsight で以下を比較:
--   SELECT COUNT(*) FROM SILVER.MEMBERS;
--   SELECT COUNT(*) FROM RETAIL_DWH.SILVER_DBT.STG_MEMBERS;
-- =============================================================================

SELECT
    -- 基本情報（VARIANT からの型変換）
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

FROM {{ source('bronze', 'members') }}
