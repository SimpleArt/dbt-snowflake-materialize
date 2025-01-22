{% macro should_drop_view(relation, sql_hash, sql) %}

    {% if execute %}

        {% for row in run_query(show_relation(relation, 'view')) %}

            {% if ('/* Query Hash: ' ~ sql_hash ~ ' */ ' ~ sql) not in row['text'] %}
                {{ return(true) }}
            {% elif not is_queryable_relation(relation) %}
                {{ return(true) }}
            {% endif %}

        {% endfor %}

    {% endif %}

    {{ return(false) }}

{% endmacro %}
