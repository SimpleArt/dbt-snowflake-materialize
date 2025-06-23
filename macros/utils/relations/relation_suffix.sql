{% macro relation_suffix(relation, suffix) %}
    {% if relation.identifier is not none %}
        {% set identifier = relation.identifier %}
    {% elif relation.schema is not none %}
        {% set identifier = relation.schema %}
    {% else %}
        {% set identifier = relation.database %}
    {% endif %}

    {% if identifier is not none %}
        {% if identifier.endswith('"') %}
            {% set identifier = identifier[:-1] ~ suffix ~ '"' %}
        {% else %}
            {% set identifier = identifier ~ suffix %}
        {% endif %}
    {% endif %}

    {% if relation.identifier is not none %}
        {{ return(relation.incorporate(path={'identifier': identifier})) }}
    {% elif relation.schema is not none %}
        {{ return(relation.incorporate(path={'schema': identifier})) }}
    {% else %}
        {{ return(relation.incorporate(path={'database': identifier})) }}
    {% endif %}
{% endmacro %}
