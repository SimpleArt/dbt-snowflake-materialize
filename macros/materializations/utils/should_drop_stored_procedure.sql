{% macro should_drop_stored_procedure(relation, sql_hash) %}

    {% if execute %}

        {% for row in run_query(show_object(database, schema, identifier, 'procedure')) %}
            {% set comment = row.get('comment', row.get('description', '')) %}

            {% if comment.endswith('Query Hash: ' ~ sql_hash) %}
                {{ return(false) }}
            {% elif loop.last %}
                {{ return(true) }}
            {% endif %}

        {% endfor %}

    {% endif %}

    {{ return(false) }}

{% endmacro %}
