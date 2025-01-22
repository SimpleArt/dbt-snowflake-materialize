{% macro should_drop_relation(relation, sql_hash, sql, type=none) %}

    {% if type is none %}
        {% set type = relation.type %}
    {% endif %}

    {% if type == 'function' %}
        {{ return(should_drop_function(relation, sql_hash)) }}

    {% elif type == 'materialized view' %}
        {{ return(should_drop_materialized_view(relation, sql_hash, sql)) }}

    {% elif type == 'procedure' %}
        {{ return(should_drop_stored_procedure(relation, sql_hash)) }}

    {% elif type == 'view' %}
        {{ return(should_drop_view(relation, sql_hash, sql)) }}

    {% else %}
        {{ return(false) }}

    {% endif %}

{% endmacro %}
