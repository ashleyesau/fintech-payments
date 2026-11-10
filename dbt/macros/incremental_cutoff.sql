{% macro incremental_cutoff(relation, column, days) %}
    {#- Literal DATE for partition pruning: MAX(column) in the target table minus N days.
        Call only inside is_incremental(). Not calendar-based, so it works on backfilled data. -#}
    {%- if execute -%}
        {%- set result = run_query("SELECT DATE_SUB(MAX(" ~ column ~ "), INTERVAL " ~ days ~ " DAY) FROM " ~ relation) -%}
        {%- set cutoff = result.columns[0].values()[0] -%}
        {%- if cutoff is none -%}DATE '1970-01-01'{%- else -%}DATE '{{ cutoff }}'{%- endif -%}
    {%- else -%}
        DATE '1970-01-01'
    {%- endif -%}
{% endmacro %}
