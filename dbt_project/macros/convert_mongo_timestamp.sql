{# =============================================================================
   MongoDB タイムスタンプ変換マクロ (Chapter 9 で学ぶ)
   =============================================================================
   【マクロとは】
   Jinja テンプレートで再利用可能な SQL の部品を定義する仕組み。
   Python の関数のように、引数を受け取って SQL 文字列を返す。

   【なぜマクロを使うのか】
   現在の SQL では CONVERT_TIMEZONE('UTC','Asia/Tokyo', ...) パターンが
   何十回も繰り返されている。マクロにすると:
   1. 1箇所修正すれば全モデルに反映される
   2. タイムゾーンの変更（例: JST→UTC）が簡単
   3. タイポのリスクが減る

   【使い方】
   {{ convert_mongo_timestamp('RAW_DATA', 'created_at') }}
   ↓ コンパイル後
   CONVERT_TIMEZONE('UTC','Asia/Tokyo', RAW_DATA:"created_at":"$date"::TIMESTAMP_NTZ)::TIMESTAMP_LTZ

   【確認方法】
   dbt compile --select stg_members  で展開後の SQL を確認できる
   ============================================================================= #}

{% macro convert_mongo_timestamp(variant_col, field_name) %}
    CONVERT_TIMEZONE(
        'UTC',
        'Asia/Tokyo',
        {{ variant_col }}:"{{ field_name }}":"$date"::TIMESTAMP_NTZ
    )::TIMESTAMP_LTZ
{% endmacro %}
