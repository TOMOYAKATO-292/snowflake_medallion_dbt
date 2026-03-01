-- =============================================================================
-- Phase L: Silver 通常テーブルの作成 (TRANSACTIONS, DAILY_MEMBER_SUMMARY)
-- =============================================================================
-- Dynamic Table ではなく通常テーブル。差分 MERGE が必要なため。
--
-- 【学べること】
-- - Dynamic Table と通常テーブルの使い分け
--   - 全量リロード → Dynamic Table（MEMBERS: 毎日全件入れ替え）
--   - 差分更新 → 通常テーブル + Stream + MERGE（TRANSACTIONS, DAILY_MEMBER_SUMMARY）
-- - PRIMARY KEY 制約（Snowflake では参考情報、強制されない）
-- =============================================================================

-- TRANSACTIONS: ID-POS 購買トランザクション（主キー: _id_str = MongoDB の $binary base64）
CREATE OR REPLACE TABLE SILVER.TRANSACTIONS (
    _id_str                   VARCHAR(200)  PRIMARY KEY,
    transaction_id            INTEGER,
    item_index                INTEGER,
    card_number               VARCHAR(1000),
    store_code                INTEGER,
    register_no               VARCHAR(1000),
    department_code           INTEGER,
    category_code             INTEGER,
    subcategory_code          INTEGER,
    transaction_date          TIMESTAMP_LTZ,
    transaction_time          VARCHAR(1000),
    jan_code                  BIGINT,
    product_name              VARCHAR(1000),
    unit_price                INTEGER,
    original_price            INTEGER,
    quantity                  INTEGER,
    weight                    DOUBLE,
    discount_amount           INTEGER,
    points_earned             INTEGER,
    tax_category              INTEGER,
    tax_rate                  INTEGER,
    amount_exclude_tax        INTEGER,
    amount_include_tax        INTEGER,
    created_at                TIMESTAMP_LTZ,
    updated_at                TIMESTAMP_LTZ,
    ym                        CHAR(6),
    ymd                       CHAR(8),
    load_date                 DATE,
    loaded_at                 TIMESTAMP_LTZ
);

-- DAILY_MEMBER_SUMMARY: 会員別日次集計（主キー: _id = "{member_id}-{YYYY-MM-DD}"）
CREATE OR REPLACE TABLE SILVER.DAILY_MEMBER_SUMMARY (
    _id                VARCHAR(100)  PRIMARY KEY,
    member_id          INTEGER,
    summary_date       TIMESTAMP_LTZ,
    visit_count        INTEGER,
    purchase_amount    INTEGER,
    item_count         INTEGER,
    points_earned      INTEGER,
    has_purchased      BOOLEAN,
    has_returned       BOOLEAN,
    created_at         TIMESTAMP_LTZ,
    updated_at         TIMESTAMP_LTZ,
    ym                 CHAR(6),
    ymd                CHAR(8),
    load_date          DATE,
    loaded_at          TIMESTAMP_LTZ
);
