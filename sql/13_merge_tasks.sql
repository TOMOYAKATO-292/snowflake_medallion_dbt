-- =============================================================================
-- Phase N: MERGE タスクと日次ロードタスクの作成
-- =============================================================================
-- タスク依存関係:
--   BRONZE.DAILY_LOAD (毎日01:00 JST) ← ルートタスク
--     ├── SILVER.MERGE_TRANSACTIONS          ← Stream にデータがあれば実行
--     ├── SILVER.MERGE_DAILY_MEMBER_SUMMARY  ← 同上
--     └── MONITORING.DATA_QUALITY_CHECK      ← Phase 15 で作成
--
-- 【学べること】
-- - Task: Snowflake のジョブスケジューラ（cron 式で定期実行）
-- - AFTER 句: タスクの依存関係（親タスク完了後に子タスクを実行）
-- - WHEN 句 + SYSTEM$STREAM_HAS_DATA: Stream にデータがある場合のみ実行
--   → 無駄な MERGE 実行を防ぎ、コストを最適化
-- =============================================================================

USE DATABASE RETAIL_DWH;
USE WAREHOUSE RETAIL_WH;

-- ルートタスク: 毎日午前1時(JST)に前日分をロード
CREATE OR REPLACE TASK BRONZE.DAILY_LOAD
    WAREHOUSE = RETAIL_WH
    SCHEDULE = 'USING CRON 0 1 * * * Asia/Tokyo'
AS
    CALL BRONZE.LOAD_ALL(DATEADD(day, -1, CURRENT_DATE()));

-- 子タスク: TRANSACTIONS の MERGE（Stream にデータがある場合のみ）
CREATE OR REPLACE TASK BRONZE.MERGE_TRANSACTIONS
    WAREHOUSE = RETAIL_WH
    AFTER BRONZE.DAILY_LOAD
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.TRANSACTIONS_STREAM')
AS
MERGE INTO SILVER.TRANSACTIONS AS tgt
USING (
  SELECT
    RAW_DATA:"_id":"$binary":"base64"::VARCHAR(200)     AS _id_str,
    RAW_DATA:"transaction_id"::INTEGER                  AS transaction_id,
    RAW_DATA:"item_index"::INTEGER                      AS item_index,
    RAW_DATA:"card_number"::VARCHAR(1000)               AS card_number,
    RAW_DATA:"store_code"::INTEGER                      AS store_code,
    RAW_DATA:"register_no"::VARCHAR(1000)               AS register_no,
    RAW_DATA:"department_code"::INTEGER                 AS department_code,
    RAW_DATA:"category_code"::INTEGER                   AS category_code,
    RAW_DATA:"subcategory_code"::INTEGER                AS subcategory_code,
    CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"transaction_date":"$date"::TIMESTAMP_NTZ)::TIMESTAMP_LTZ  AS transaction_date,
    RAW_DATA:"transaction_time"::VARCHAR(1000)          AS transaction_time,
    RAW_DATA:"jan_code":"$numberLong"::BIGINT           AS jan_code,
    RAW_DATA:"product_name"::VARCHAR(1000)              AS product_name,
    RAW_DATA:"unit_price"::INTEGER                      AS unit_price,
    RAW_DATA:"original_price"::INTEGER                  AS original_price,
    RAW_DATA:"quantity"::INTEGER                        AS quantity,
    RAW_DATA:"weight"::DOUBLE                           AS weight,
    RAW_DATA:"discount_amount"::INTEGER                 AS discount_amount,
    RAW_DATA:"points_earned"::INTEGER                   AS points_earned,
    RAW_DATA:"tax_category"::INTEGER                    AS tax_category,
    RAW_DATA:"tax_rate"::INTEGER                        AS tax_rate,
    RAW_DATA:"amount_exclude_tax"::INTEGER              AS amount_exclude_tax,
    RAW_DATA:"amount_include_tax"::INTEGER              AS amount_include_tax,
    CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"created_at":"$date"::TIMESTAMP_NTZ)::TIMESTAMP_LTZ       AS created_at,
    CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"updated_at":"$date"::TIMESTAMP_NTZ)::TIMESTAMP_LTZ       AS updated_at,
    TO_CHAR(CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"transaction_date":"$date"::TIMESTAMP_NTZ), 'YYYYMM')   AS ym,
    TO_CHAR(CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"transaction_date":"$date"::TIMESTAMP_NTZ), 'YYYYMMDD') AS ymd,
    LOAD_DATE,
    LOADED_AT
  FROM BRONZE.TRANSACTIONS_STREAM
  WHERE METADATA$ACTION = 'INSERT'
) AS src
ON tgt._id_str = src._id_str
WHEN MATCHED AND src.updated_at > tgt.updated_at THEN UPDATE SET
  transaction_id = src.transaction_id, item_index = src.item_index,
  card_number = src.card_number, store_code = src.store_code,
  register_no = src.register_no, department_code = src.department_code,
  category_code = src.category_code, subcategory_code = src.subcategory_code,
  transaction_date = src.transaction_date, transaction_time = src.transaction_time,
  jan_code = src.jan_code, product_name = src.product_name,
  unit_price = src.unit_price, original_price = src.original_price,
  quantity = src.quantity, weight = src.weight,
  discount_amount = src.discount_amount, points_earned = src.points_earned,
  tax_category = src.tax_category, tax_rate = src.tax_rate,
  amount_exclude_tax = src.amount_exclude_tax, amount_include_tax = src.amount_include_tax,
  created_at = src.created_at, updated_at = src.updated_at,
  ym = src.ym, ymd = src.ymd, load_date = src.load_date, loaded_at = src.loaded_at
WHEN NOT MATCHED THEN INSERT VALUES (
  src._id_str, src.transaction_id, src.item_index,
  src.card_number, src.store_code, src.register_no,
  src.department_code, src.category_code, src.subcategory_code,
  src.transaction_date, src.transaction_time,
  src.jan_code, src.product_name,
  src.unit_price, src.original_price, src.quantity, src.weight,
  src.discount_amount, src.points_earned,
  src.tax_category, src.tax_rate,
  src.amount_exclude_tax, src.amount_include_tax,
  src.created_at, src.updated_at, src.ym, src.ymd,
  src.load_date, src.loaded_at
);

-- 子タスク: DAILY_MEMBER_SUMMARY の MERGE（Stream にデータがある場合のみ）
CREATE OR REPLACE TASK BRONZE.MERGE_DAILY_MEMBER_SUMMARY
    WAREHOUSE = RETAIL_WH
    AFTER BRONZE.DAILY_LOAD
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.DAILY_MEMBER_SUMMARY_STREAM')
AS
MERGE INTO SILVER.DAILY_MEMBER_SUMMARY AS tgt
USING (
  SELECT
    RAW_DATA:"_id"::VARCHAR(100)                        AS _id,
    RAW_DATA:"member_id"::INTEGER                       AS member_id,
    CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"summary_date":"$date"::TIMESTAMP_NTZ)::TIMESTAMP_LTZ  AS summary_date,
    RAW_DATA:"visit_count"::INTEGER                     AS visit_count,
    RAW_DATA:"purchase_amount"::INTEGER                 AS purchase_amount,
    RAW_DATA:"item_count"::INTEGER                      AS item_count,
    RAW_DATA:"points_earned"::INTEGER                   AS points_earned,
    RAW_DATA:"has_purchased"::BOOLEAN                   AS has_purchased,
    RAW_DATA:"has_returned"::BOOLEAN                    AS has_returned,
    CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"created_at":"$date"::TIMESTAMP_NTZ)::TIMESTAMP_LTZ    AS created_at,
    CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"updated_at":"$date"::TIMESTAMP_NTZ)::TIMESTAMP_LTZ    AS updated_at,
    TO_CHAR(CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"created_at":"$date"::TIMESTAMP_NTZ), 'YYYYMM')   AS ym,
    TO_CHAR(CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"created_at":"$date"::TIMESTAMP_NTZ), 'YYYYMMDD') AS ymd,
    LOAD_DATE,
    LOADED_AT
  FROM BRONZE.DAILY_MEMBER_SUMMARY_STREAM
  WHERE METADATA$ACTION = 'INSERT'
) AS src
ON tgt._id = src._id
WHEN MATCHED AND src.updated_at > tgt.updated_at THEN UPDATE SET
  member_id = src.member_id, summary_date = src.summary_date,
  visit_count = src.visit_count, purchase_amount = src.purchase_amount,
  item_count = src.item_count, points_earned = src.points_earned,
  has_purchased = src.has_purchased, has_returned = src.has_returned,
  created_at = src.created_at, updated_at = src.updated_at,
  ym = src.ym, ymd = src.ymd, load_date = src.load_date, loaded_at = src.loaded_at
WHEN NOT MATCHED THEN INSERT VALUES (
  src._id, src.member_id, src.summary_date,
  src.visit_count, src.purchase_amount, src.item_count, src.points_earned,
  src.has_purchased, src.has_returned,
  src.created_at, src.updated_at, src.ym, src.ymd,
  src.load_date, src.loaded_at
);
