{% macro setup_functions_and_procs() %}
    {% do setup_materialized_functions() %}
    {% do setup_describe_table() %}
    {% do setup_show_streams() %}
    {{ return('select 1 as no_op') }}
{% endmacro %}
