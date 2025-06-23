{% macro drop_relation(relation, unless_type=none, query=none, check_queryable=false) %}
    {{ return(adapter.dispatch('drop_relation')(relation, unless_type, query, check_queryable)) }}
{% endmacro %}

{% macro snowflake__drop_relation(relation, unless_type, query, check_queryable) %}

    {% set result = get_relation_type(relation, unless_type, query) %}
    {% set type = result.get('type') %}
    {% set rows = result.get('rows') %}

    {% if unless_type is none %}
        {% set unless_type = relation.type %}
    {% endif %}

    {% if type is none or not execute %}
        {{ return({'DDL': 'create or replace', 'type': type}) }}

    {% elif type in ['function', 'procedure'] %}
        {% if type == unless_type and not flags.FULL_REFRESH %}
            {% for row in rows %}
                {{ return({'DDL': row.get('DDL', 'create or replace'), 'type': type, 'rows': rows}) }}
            {% endfor %}
        {% endif %}

        {% for row in rows %}
            {% call statement('drop_' ~ type) %}
                drop {{ type }} if exists {{ relation }}{{ row['arguments_no_return'] }})
            {% endcall %}
        {% endfor %}

        {{ return({'DDL': 'create or replace', 'type': type}) }}

    {% elif type != unless_type or flags.FULL_REFRESH %}
        {% call statement('drop_object') %}
            drop {{ type }} if exists {{ relation }}
        {% endcall %}

        {{ return({'DDL': 'create or replace', 'type': type}) }}

    {% endif %}

    {% if check_queryable and not is_queryable(relation) %}
        {{ return({'DDL': 'create or replace', 'type': type, 'rows': rows}) }}
    {% endif %}

    {% for row in rows %}
        {{ return({'DDL': row.get('DDL', 'create or replace'), 'type': type, 'rows': rows}) }}
    {% endfor %}

    {{ return({'DDL': 'create or replace', 'type': type}) }}

{% endmacro %}
