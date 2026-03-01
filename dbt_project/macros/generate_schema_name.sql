-- =============================================================================
-- カスタムスキーマ名マクロ
-- =============================================================================
-- dbt のデフォルトでは custom_schema を設定すると
--   <default_schema>_<custom_schema> （例: PUBLIC_SILVER_DBT）
-- と結合されてしまう。
--
-- このマクロで custom_schema が指定されている場合は
-- そのまま使うように上書きする（例: SILVER_DBT）。
-- =============================================================================

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
