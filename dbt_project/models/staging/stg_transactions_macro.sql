-- =============================================================================
-- Silver Transactions モデル（マクロ適用版）
-- =============================================================================
-- stg_transactions.sql のリファクタ版。
-- CONVERT_TIMEZONE パターンを convert_mongo_timestamp マクロに、
-- ym/ymd 生成を generate_ym_ymd マクロに置き換えている。
--
-- 【注意】ym/ymd は取引日(transaction_date)ベースで生成している（created_at ではない）。
-- =============================================================================

{{
    config(
        materialized='incremental',
        unique_key='_id_str',
        merge_update_columns=[
            'transaction_id', 'item_index',
            'card_number', 'store_code', 'register_no',
            'department_code', 'category_code', 'subcategory_code',
            'transaction_date', 'transaction_time',
            'jan_code', 'product_name',
            'unit_price', 'original_price', 'quantity', 'weight',
            'discount_amount', 'points_earned',
            'tax_category', 'tax_rate',
            'amount_exclude_tax', 'amount_include_tax',
            'created_at', 'updated_at', 'ym', 'ymd', 'load_date', 'loaded_at'
        ]
    )
}}

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY RAW_DATA:"_id":"$binary":"base64"::VARCHAR(200)
            ORDER BY LOADED_AT DESC
        ) AS _rn
    FROM {{ source('bronze', 'transactions') }}
    {% if is_incremental() %}
        WHERE LOADED_AT > (SELECT MAX(LOADED_AT) FROM {{ this }})
    {% endif %}
)

SELECT
    -- 主キー（MongoDB の $binary base64 形式）
    RAW_DATA:"_id":"$binary":"base64"::VARCHAR(200)     AS _id_str,

    -- トランザクション情報
    RAW_DATA:"transaction_id"::INTEGER                  AS transaction_id,
    RAW_DATA:"item_index"::INTEGER                      AS item_index,

    -- カード・店舗情報
    RAW_DATA:"card_number"::VARCHAR(1000)               AS card_number,
    RAW_DATA:"store_code"::INTEGER                      AS store_code,
    RAW_DATA:"register_no"::VARCHAR(1000)               AS register_no,
    RAW_DATA:"department_code"::INTEGER                 AS department_code,
    RAW_DATA:"category_code"::INTEGER                   AS category_code,
    RAW_DATA:"subcategory_code"::INTEGER                AS subcategory_code,

    -- 日時（マクロで UTC → JST 変換）
    {{ convert_mongo_timestamp('RAW_DATA', 'transaction_date') }}  AS transaction_date,
    RAW_DATA:"transaction_time"::VARCHAR(1000)          AS transaction_time,

    -- 商品情報
    RAW_DATA:"jan_code":"$numberLong"::BIGINT           AS jan_code,
    RAW_DATA:"product_name"::VARCHAR(1000)              AS product_name,
    RAW_DATA:"unit_price"::INTEGER                      AS unit_price,
    RAW_DATA:"original_price"::INTEGER                  AS original_price,
    RAW_DATA:"quantity"::INTEGER                        AS quantity,
    RAW_DATA:"weight"::DOUBLE                           AS weight,

    -- 値引・ポイント
    RAW_DATA:"discount_amount"::INTEGER                 AS discount_amount,
    RAW_DATA:"points_earned"::INTEGER                   AS points_earned,

    -- 税情報
    RAW_DATA:"tax_category"::INTEGER                    AS tax_category,
    RAW_DATA:"tax_rate"::INTEGER                        AS tax_rate,
    RAW_DATA:"amount_exclude_tax"::INTEGER              AS amount_exclude_tax,
    RAW_DATA:"amount_include_tax"::INTEGER              AS amount_include_tax,

    -- タイムスタンプ（マクロで UTC → JST 変換）
    {{ convert_mongo_timestamp('RAW_DATA', 'created_at') }}  AS created_at,
    {{ convert_mongo_timestamp('RAW_DATA', 'updated_at') }}  AS updated_at,

    -- パーティションキー（取引日ベース、マクロで ym/ymd 生成）
    {{ generate_ym_ymd('RAW_DATA', 'transaction_date') }},

    -- メタデータ
    LOAD_DATE,
    LOADED_AT

FROM ranked
WHERE _rn = 1
