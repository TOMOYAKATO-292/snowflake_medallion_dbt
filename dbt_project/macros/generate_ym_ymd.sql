{# =============================================================================
   年月・年月日カラム生成マクロ (Chapter 9 で学ぶ)
   =============================================================================
   ym (YYYYMM) と ymd (YYYYMMDD) を MongoDB の $date フィールドから生成する。
   パーティショニングや集計に使う共通パターン。

   【使い方】
   {{ generate_ym_ymd('RAW_DATA', 'created_at') }}
   ↓ コンパイル後
   TO_CHAR(CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"created_at":"$date"::TIMESTAMP_NTZ), 'YYYYMM') AS ym,
   TO_CHAR(CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"created_at":"$date"::TIMESTAMP_NTZ), 'YYYYMMDD') AS ymd
   ============================================================================= #}

{% macro generate_ym_ymd(variant_col, field_name) %}
    TO_CHAR(
        CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', {{ variant_col }}:"{{ field_name }}":"$date"::TIMESTAMP_NTZ),
        'YYYYMM'
    ) AS ym,
    TO_CHAR(
        CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', {{ variant_col }}:"{{ field_name }}":"$date"::TIMESTAMP_NTZ),
        'YYYYMMDD'
    ) AS ymd
{% endmacro %}
