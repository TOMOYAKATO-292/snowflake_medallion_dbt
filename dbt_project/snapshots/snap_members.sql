-- =============================================================================
-- Snapshot: 会員変更履歴 (Chapter 10 で学ぶ — ボーナス)
-- =============================================================================
-- 【Snapshot（SCD Type 2）とは】
-- テーブルのレコードが変更されたとき、その履歴を自動的に記録する仕組み。
-- SCD = Slowly Changing Dimension（緩やかに変化するディメンション）。
--
-- 例: 会員が名前を "太郎" → "花子" に変更した場合:
-- | member_id | member_name | dbt_valid_from      | dbt_valid_to        |
-- |-----------|-------------|---------------------|---------------------|
-- | 1         | 太郎        | 2026-02-15 00:00:00 | 2026-02-16 00:00:00 |
-- | 1         | 花子        | 2026-02-16 00:00:00 | NULL                |
--
-- dbt_valid_to が NULL = 現在有効なレコード。
--
-- 【strategy: timestamp】
-- updated_at カラムの値が変わったら「変更あり」と判定する。
-- もう一つの strategy は check（指定カラムの値を比較）。
--
-- 【実行方法】
-- dbt snapshot                    # スナップショット実行
-- 初回: 全レコードが dbt_valid_to = NULL で挿入される
-- 2回目以降: 変更されたレコードの旧バージョンに dbt_valid_to がセットされ、
--           新バージョンが dbt_valid_to = NULL で挿入される
--
-- 【検証方法】
-- 1. dbt snapshot を実行
-- 2. SELECT * FROM RETAIL_DWH.SNAPSHOTS.SNAP_MEMBERS WHERE member_id = '1';
-- 3. Bronze にデータをリロード（会員データが更新されている想定）
-- 4. もう一度 dbt snapshot を実行
-- 5. 同じクエリで履歴が記録されていることを確認
-- =============================================================================

{% snapshot snap_members %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='member_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

SELECT * FROM {{ ref('stg_members') }}

{% endsnapshot %}
