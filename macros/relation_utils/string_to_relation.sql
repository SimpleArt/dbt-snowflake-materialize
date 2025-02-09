{% macro string_to_relation(sql) %}

    {% if sql is not string %}
        {{ return(sql) }}
    {% endif %}

    {% set parts = [] %}
    {% set state = {'temp': ''} %}

    {% for part in sql.split('.') %}
        {% if state['temp'] == '' and part == '' %}
            {{ return(none) }}
        {% elif state['temp'] == '' and part[0] != '"' %}
            {% do parts.append(part) %}
        {% elif state['temp'] == '' %}
            {% do state.update({'temp': part}) %}
        {% elif part != '' and part[0] == '"' %}
            {% do parts.append(state['part'] ~ '.' ~ part) %}
        {% else %}
            {% do state.update({'temp': state['part'] ~ '.' ~ part}) %}
        {% endif %}
    {% endfor %}

    {% if state['temp'] != '' %}
        {{ return(none) }}
    {% elif parts | length == 1 %}
        {{ return(api.Relation.create(
            database=none,
            schema=none,
            identifier=parts[0]
        )) }}
    {% elif parts | length == 2 %}
        {{ return(api.Relation.create(
            database=none,
            schema=parts[0],
            identifier=parts[1]
        )) }}
    {% elif parts | length == 3 %}
        {{ return(api.Relation.create(
            database=parts[0],
            schema=parts[1],
            identifier=parts[2]
        )) }}
    {% else %}
        {{ return(none) }}
    {% endif %}

{% endmacro %}
