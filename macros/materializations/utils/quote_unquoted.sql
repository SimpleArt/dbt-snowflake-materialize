{% macro quote_unquoted(identifier) %}

    {% if '"' in identifier %}
        {{ return(identifier.strip().strip('"')) }}
    {% else %}
        {{ return(identifier.upper()) }}
    {% endif %}

{% endmacro %}
