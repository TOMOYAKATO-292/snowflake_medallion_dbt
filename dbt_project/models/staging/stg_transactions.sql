-- =============================================================================
-- Silver Transactions モデル (Chapter 6 で学ぶ — 最重要チャプター)
-- =============================================================================
-- 【このモデルの役割】
-- Bronze.TRANSACTIONS の RAW_DATA から ID-POS 購買トランザクションを抽出・型変換する。
-- 元の MERGE INTO SILVER.TRANSACTIONS (12_initial_merge.sql, 13_merge_tasks.sql) を置き換える。
--
-- 【materialization: incremental（増分モデル）】
-- dbt の最も強力な機能の一つ。大量データの変換を効率化する。
--
-- ■ 初回実行 (dbt run --select stg_transactions):
--   CREATE TABLE AS SELECT ... を実行（全データをロード）
--
-- ■ 2回目以降:
--   テーブルが既に存在するため、is_incremental() が TRUE になる。
--   WHERE LOADED_AT > (SELECT MAX(LOADED_AT) FROM this) で新規データのみ抽出し、
--   MERGE INTO で既存テーブルに差分適用する。
--
-- ■ フルリフレッシュ (dbt run --select stg_transactions --full-refresh):
--   テーブルを DROP して再作成。データの整合性がおかしくなったときに使う。
--
-- 【merge 戦略と unique_key】
-- unique_key = '_id_str' により、同じ _id_str のレコードが来たら UPDATE される。
--
-- 【検証方法】
-- 1. dbt run --select stg_transactions          # 初回フルロード
-- 2. Snowsight で比較:
--    SELECT COUNT(*) FROM SILVER.TRANSACTIONS;
--    SELECT COUNT(*) FROM RETAIL_DWH.SILVER_DBT.STG_TRANSACTIONS;
-- 3. Bronze にデルタデータをロード後:
--    CALL BRONZE.LOAD_ALL('2026-02-16');
-- 4. dbt run --select stg_transactions          # 増分マージ
-- 5. 再度行数を比較して一致を確認
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

-- 重複排除: Bronze に同じ _id_str が複数バージョン存在する場合、
-- LOADED_AT が最新のレコードのみを採用する（Silver 層の責務）。
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

    -- 日時
    CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"transaction_date":"$date"::TIMESTAMP_NTZ)::TIMESTAMP_LTZ  AS transaction_date,
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

    -- タイムスタンプ（UTC → JST 変換）
    CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"created_at":"$date"::TIMESTAMP_NTZ)::TIMESTAMP_LTZ       AS created_at,
    CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"updated_at":"$date"::TIMESTAMP_NTZ)::TIMESTAMP_LTZ       AS updated_at,

    -- パーティションキー（取引日ベース）
    TO_CHAR(CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"transaction_date":"$date"::TIMESTAMP_NTZ), 'YYYYMM')   AS ym,
    TO_CHAR(CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"transaction_date":"$date"::TIMESTAMP_NTZ), 'YYYYMMDD') AS ymd,

    -- メタデータ
    LOAD_DATE,
    LOADED_AT

FROM ranked
WHERE _rn = 1
