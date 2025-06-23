{% macro show_relation(relation, type='object') %}
    {% if type is none %}
        {% set type = relation.type %}
    {% endif %}

    {{ return(adapter.dispatch('show_relation')(relation, type)) }}
{% endmacro %}

{%- macro default__show_relation(relation, type) -%}
    {% set relation = get_fully_qualified_relation(relation) %}
    {%- if relation.identifier is not none -%}
        show {{ (type ~ 's').replace('ys', 'ies') }} like '{{ escape_ansii(relation.identifier) }}' in {{ relation.include(identifier=false) }}
    {%- elif relation.schema is not none -%}
        show {{ (type ~ 's').replace('ys', 'ies') }} like '{{ escape_ansii(relation.schema) }}' in database {{ relation.database }}
    {%- else -%}
        show {{ (type ~ 's').replace('ys', 'ies') }} like '{{ escape_ansii(relation.database) }}'
    {%- endif -%}
{%- endmacro -%}
