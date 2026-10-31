-- Override dbt's default schema-naming behaviour.
--
-- By default dbt prefixes the custom schema with the target schema:
-- e.g. target=staging, +schema=marts -> staging_marts
--
-- This override uses the custom schema name as-is, so +schema=marts gives
-- the dataset marts, and +schema=staging gives staging. Models with no
-- custom schema land in the target dataset from profiles.yml.

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
