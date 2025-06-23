{% macro is_non_string_iterable(L) %}
    {{ return(L is iterable and L is not string) }}
{% endmacro %}
