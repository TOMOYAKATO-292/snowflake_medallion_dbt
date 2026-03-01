-- =============================================================================
-- Silver Members モデル（マクロ適用版）
-- =============================================================================
-- stg_members.sql のリファクタ版。
-- CONVERT_TIMEZONE パターンを convert_mongo_timestamp マクロに、
-- ym/ymd 生成を generate_ym_ymd マクロに置き換えている。
--
-- 【ベタ書き版との比較ポイント】
-- - タイムスタンプ変換: 3行 → マクロ呼び出し3回（パターン統一）
-- - ym/ymd 生成: 2行 → マクロ呼び出し1回
-- - タイムゾーンや $date パスの変更が macros/ の1箇所で済む
--
-- 【確認方法】
-- dbt compile --select stg_members_macro  で展開後 SQL を確認し、
-- stg_members の compile 結果と比較する。
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

    -- タイムスタンプ（マクロで UTC → JST 変換）
    {{ convert_mongo_timestamp('RAW_DATA', 'registered_at') }}   AS registered_at,
    {{ convert_mongo_timestamp('RAW_DATA', 'last_visit_at') }}   AS last_visit_at,
    {{ convert_mongo_timestamp('RAW_DATA', 'updated_at') }}      AS updated_at,

    -- パーティションキー（マクロで ym/ymd 生成）
    {{ generate_ym_ymd('RAW_DATA', 'registered_at') }},

    -- メタデータ
    LOAD_DATE

FROM {{ source('bronze', 'members') }}
