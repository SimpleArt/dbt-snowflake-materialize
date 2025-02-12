{% macro setup_functions_and_procs() %}
    {% do setup_materialized_functions() %}
    {% do setup_describe_table() %}
    {% do setup_describe_view() %}
    {{ return('select 1 as no_op') }}
{% endmacro %}
