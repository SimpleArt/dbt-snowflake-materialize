{% macro join_strings(x) %}
    {% if x is iterable and x is not string %}
        {{ return(x | join(', ')) }}
    {% else %}
        {{ return(x) }}
    {% endif %}
{% endmacro %}
