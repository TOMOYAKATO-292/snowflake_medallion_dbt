-- =============================================================================
-- Silver Daily Member Summary モデル (Chapter 6 で学ぶ)
-- =============================================================================
-- 【このモデルの役割】
-- Bronze.DAILY_MEMBER_SUMMARY の RAW_DATA から会員別日次集計を抽出・型変換する。
-- 元の MERGE INTO SILVER.DAILY_MEMBER_SUMMARY (12_initial_merge.sql) を置き換える。
--
-- 【stg_transactions との違い】
-- - unique_key が '_id'（形式: "{member_id}-{YYYY-MM-DD}"）
-- - 更新率が約98%と非常に高い（ほとんどのレコードが MERGE で UPDATE される）
-- - それでも dbt incremental の merge 戦略で正しく処理される
--
-- 【検証方法】
-- dbt run --select stg_daily_member_summary
-- SELECT COUNT(*) FROM SILVER.DAILY_MEMBER_SUMMARY;
-- SELECT COUNT(*) FROM RETAIL_DWH.SILVER_DBT.STG_DAILY_MEMBER_SUMMARY;
-- =============================================================================

{{
    config(
        materialized='incremental',
        unique_key='_id',
        merge_update_columns=[
            'member_id', 'summary_date',
            'visit_count', 'purchase_amount', 'item_count', 'points_earned',
            'has_purchased', 'has_returned',
            'created_at', 'updated_at',
            'ym', 'ymd', 'load_date', 'loaded_at'
        ]
    )
}}

-- 重複排除: Bronze に同じ _id が複数バージョン存在する場合、
-- LOADED_AT が最新のレコードのみを採用する（Silver 層の責務）。
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY RAW_DATA:"_id"::VARCHAR(100)
            ORDER BY LOADED_AT DESC
        ) AS _rn
    FROM {{ source('bronze', 'daily_member_summary') }}
    {% if is_incremental() %}
        WHERE LOADED_AT > (SELECT MAX(LOADED_AT) FROM {{ this }})
    {% endif %}
)

SELECT
    -- 主キー（形式: "{member_id}-{YYYY-MM-DD}"）
    RAW_DATA:"_id"::VARCHAR(100)                        AS _id,

    -- 会員情報
    RAW_DATA:"member_id"::INTEGER                       AS member_id,

    -- 集計対象日
    CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"summary_date":"$date"::TIMESTAMP_NTZ)::TIMESTAMP_LTZ  AS summary_date,

    -- 集計データ
    RAW_DATA:"visit_count"::INTEGER                     AS visit_count,
    RAW_DATA:"purchase_amount"::INTEGER                 AS purchase_amount,
    RAW_DATA:"item_count"::INTEGER                      AS item_count,
    RAW_DATA:"points_earned"::INTEGER                   AS points_earned,

    -- フラグ
    RAW_DATA:"has_purchased"::BOOLEAN                   AS has_purchased,
    RAW_DATA:"has_returned"::BOOLEAN                    AS has_returned,

    -- タイムスタンプ（UTC → JST 変換）
    CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"created_at":"$date"::TIMESTAMP_NTZ)::TIMESTAMP_LTZ    AS created_at,
    CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"updated_at":"$date"::TIMESTAMP_NTZ)::TIMESTAMP_LTZ    AS updated_at,

    -- パーティションキー
    TO_CHAR(CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"created_at":"$date"::TIMESTAMP_NTZ), 'YYYYMM')   AS ym,
    TO_CHAR(CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"created_at":"$date"::TIMESTAMP_NTZ), 'YYYYMMDD') AS ymd,

    -- メタデータ
    LOAD_DATE,
    LOADED_AT

FROM ranked
WHERE _rn = 1
