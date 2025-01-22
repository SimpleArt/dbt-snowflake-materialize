{% macro post_persist_docs(relation, model, type=none) %}

    {% if execute %}
        {% set descriptions = get_yaml_descriptions(model) %}

        {% if descriptions %}
            {% for row in run_query(describe_relation(relation, type)) %}
                {% set name = row.get('name') %}
                {% set description = descriptions.get(name) %}
                {% set comment = row.get('comment', row.get('description')) %}

                {% if name is not none and description is not none and description != comment %}
                    {% do run_query(comment_column(relation, adapter.quote(name), description)) %}
                {% endif %}
            {% endfor %}
        {% endif %}

        {% if model.description %}
            {% do run_query(comment_model(relation, model.description, type)) %}
        {% endif %}
    {% endif %}

    {{ return(none) }}

{% endmacro %}
