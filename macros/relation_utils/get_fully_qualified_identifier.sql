{% macro get_fully_qualified_identifier(identifier) %}
    {{ return(adapter.dispatch('get_fully_qualified_identifier')(identifier)) }}
{% endmacro %}

{% macro default__get_fully_qualified_identifier(identifier) %}
    {{ return(identifier) }}
{% endmacro %}

{% macro snowflake__get_fully_qualified_identifier(identifier) %}
    {% if identifier is none %}
        {{ return(none) }}
    {% elif identifier.endswith('"') %}
        {{ return(identifier) }}
    {% else %}
        {{ return(identifier.upper()) }}
    {% endif %}
{% endmacro %}
