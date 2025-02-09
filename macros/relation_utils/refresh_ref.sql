{% macro refresh_ref(arg1, arg2) %}
    {% set relation = make_temp_relation(this) %}
    {% set relation = get_fully_qualified_relation(relation) %}
    {{ return(relation.incorporate(type='view')) }}
{% endmacro %}
