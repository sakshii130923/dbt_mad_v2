{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Override default dbt behavior.
        If target name is 'dev', prepend 'dev_' to the custom schema name.
        Otherwise (e.g. production), output the custom schema name without prefix.
        If no custom schema is specified, fall back to target schema.
    #}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {%- if target.name == 'dev' -%}
            dev_{{ custom_schema_name | trim }}
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
