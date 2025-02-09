{% macro custom_persist_docs(relation, model, type=none, metadata=none, arguments=none) %}
    {% if type is none %}
        {% set type = relation.type %}
    {% endif %}

    {% if execute %}
        {% set descriptions = get_yaml_descriptions(model) %}

        {% if descriptions %}
            {% call statement('comment_columns') %}
                alter {{ type }} {{ relation }} alter (
                    {%- for column, comment in descriptions.items() %}
                    column {{ adapter.quote(column) }} comment $${{ comment }}$$ {{- ',' if not loop.last }}
                    {%- endfor %}
                )
            {% endcall %}
        {% endif %}

        {% if model.description %}
            {% call statement('comment_relation') %}
                alter {{ type }} {{ relation }} {{- arguments if arguments is not none }} set comment = $${{ model.description }} {{- '\n' ~ metadata if metadata is not none }}$$
            {% endcall %}
        {% endif %}
    {% endif %}

    {{ return(none) }}

{% endmacro %}
