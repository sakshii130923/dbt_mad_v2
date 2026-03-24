{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Override default dbt behavior.
        If a custom schema is specified (e.g. +schema: staging),
        use ONLY that schema name, not <target_schema>_<custom_schema>.
        Otherwise fall back to the target schema (e.g. dev, prod).
    #}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
