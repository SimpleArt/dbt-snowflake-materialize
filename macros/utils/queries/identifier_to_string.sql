{# identifier_to_string('"ColumnName"') == 'ColumnName' #}
{# identifier_to_string('COLUMN_NAME') == 'COLUMN_NAME' #}

{% macro identifier_to_string(identifier) %}
    {% set identifier = identifier | string %}
    {% if identifier.startswith('"') and identifier.endswith('"') %}
        {% do return(escape_ansii(identifier[1:][:-1])) %}
    {% else %}
        {% do return(escape_ansii(identifier)) %}
    {% endif %}
{% endmacro %}
