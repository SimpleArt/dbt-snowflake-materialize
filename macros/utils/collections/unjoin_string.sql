{% macro unjoin_strings(obj) %}
    {% if obj is string %}
        {{ return([obj]) }}
    {% else %}
        {{ return(obj) }}
    {% endif %}
{% endmacro %}
