{% macro should_drop_materialized_view(relation, sql_hash, sql) %}

    {% if execute %}

        {% for row in run_query(show_relation(relation, 'materialized view'))
            if row['invalid'] == 'true'
            or ('/* Query Hash: ' ~ sql_hash ~ ' */ ' ~ sql) not in row['text'] %}
                {{ return(true) }}
        {% endfor %}

    {% endif %}

    {{ return(false) }}

{% endmacro %}
