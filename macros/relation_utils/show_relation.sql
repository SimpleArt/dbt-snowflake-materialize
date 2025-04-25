{% macro show_relation(relation, type=none) %}
    {% if type is none %}
        {% set type = relation.type %}
    {% endif %}

    {{ return(adapter.dispatch('show_relation')(relation, type)) }}
{% endmacro %}

{%- macro default__show_relation(relation, type) -%}
    {%- if relation.identifier is not none -%}
        show {{ (type ~ 's').replace('ys', 'ies') }} like $${{ relation.identifier }}$$ in {{ relation.include(identifier=false) }}
    {%- elif relation.schema is not none -%}
        show schemas like $${{ relation.schema }}$$ in database {{ relation.database }}
    {%- else -%}
        show databases like $${{ relation.database }}$$
    {%- endif -%}
{%- endmacro -%}
