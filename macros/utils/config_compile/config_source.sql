{% macro config_source(source_name, table_name) %}
    {% set result = {'source': source_name, 'table': table_name} %}

    {% do source(source_name, table_name) %}

    {% if kwargs.get('file') is string %}
        {% set result = result ~ '/' ~ kwargs.get('file') %}
    {% endif %}

    {% if kwargs.get('string') %}
        {% set result = {'string': result} %}
    {% endif %}

    {{ return(result) }}
{% endmacro %}
