-- =============================================================================
-- Silver Daily Member Summary モデル（マクロ適用版）
-- =============================================================================
-- stg_daily_member_summary.sql のリファクタ版。
-- CONVERT_TIMEZONE パターンを convert_mongo_timestamp マクロに、
-- ym/ymd 生成を generate_ym_ymd マクロに置き換えている。
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

    -- 集計対象日（マクロで UTC → JST 変換）
    {{ convert_mongo_timestamp('RAW_DATA', 'summary_date') }}   AS summary_date,

    -- 集計データ
    RAW_DATA:"visit_count"::INTEGER                     AS visit_count,
    RAW_DATA:"purchase_amount"::INTEGER                 AS purchase_amount,
    RAW_DATA:"item_count"::INTEGER                      AS item_count,
    RAW_DATA:"points_earned"::INTEGER                   AS points_earned,

    -- フラグ
    RAW_DATA:"has_purchased"::BOOLEAN                   AS has_purchased,
    RAW_DATA:"has_returned"::BOOLEAN                    AS has_returned,

    -- タイムスタンプ（マクロで UTC → JST 変換）
    {{ convert_mongo_timestamp('RAW_DATA', 'created_at') }}       AS created_at,
    {{ convert_mongo_timestamp('RAW_DATA', 'updated_at') }}       AS updated_at,

    -- パーティションキー（マクロで ym/ymd 生成）
    {{ generate_ym_ymd('RAW_DATA', 'created_at') }},

    -- メタデータ
    LOAD_DATE,
    LOADED_AT

FROM ranked
WHERE _rn = 1
