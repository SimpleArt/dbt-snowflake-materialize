{% macro f_string_sql(sql) %}
    {% set results = [] %}

    {% for part in sql.split('{') %}
        {% do results.append(
            part
                .replace('}', '{chr(125)}')
                .replace(';', '{chr(59)}')
                .replace("'", '{chr(39)}')
                .replace('$', '{chr(36)}')
                .replace('"', '{chr(34)}')
        ) %}
    {% endfor %}

    {{ return(results | join('{chr(123)}')) }}
{% endmacro %}
