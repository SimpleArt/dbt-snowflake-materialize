{# get_fully_qualified_identifier('my_table') == 'MY_TABLE' #}
{# get_fully_qualified_identifier('"my_table"') == '"my_table"' #}

{% macro get_fully_qualified_identifier(identifier) %}
    {{ return(adapter.dispatch('get_fully_qualified_identifier')(identifier)) }}
{% endmacro %}

{% macro default__get_fully_qualified_identifier(identifier) %}
    {{ return(identifier) }}
{% endmacro %}

{% macro snowflake__get_fully_qualified_identifier(identifier) %}
    {% if identifier is not string %}
        {{ return(none) }}
    {% elif identifier.startswith('"') and identifier.endswith('"') %}
        {{ return(identifier) }}
    {% else %}
        {{ return(identifier.upper()) }}
    {% endif %}
{% endmacro %}
