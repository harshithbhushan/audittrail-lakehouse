-- dbt's default behavior concatenates a custom +schema with the profile's
-- base schema (e.g. "staging_marts"), which isn't what we want here --
-- override it to use the custom schema name directly when one is set
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
