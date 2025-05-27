{% macro setup_functions_and_procs() %}
    {% do setup_materialized_functions() %}
    {{ return('select 1 as no_op') }}
{% endmacro %}
