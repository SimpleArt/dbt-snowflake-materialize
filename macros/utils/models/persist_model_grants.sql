{% macro persist_model_docs(relation, model, type=none, metadata=none) %}
    {% if type is none %}
        {% set type = relation.type %}
    {% endif %}

    {% if execute %}
        {% set descriptions = get_yaml_descriptions(model) %}
    {% endif %}

    {% if execute and (descriptions or model.description) %}
        {% call statement('comment_model') %}
        {%- if descriptions %}
            alter {{ type }} {{ relation }} alter (
                {%- for column, comment in descriptions.items() %}
                column {{ adapter.quote(column) }} comment '{{ escape_ansii(comment) }}' {{- ',' if not loop.last }}
                {%- endfor %}
            )
        {%- if model.description %}
        ->>
        {%- endif %}
        {%- endif %}
        {%- if model.description %}
            alter {{ type }} {{ relation }} {{- arguments if arguments is not none }} set
                comment = '{{ escape_ansii(model.description) }}
                    {{- '\\n' ~ escape_ansii(metadata) if metadata is not none }}'
        {%- endif %}
        {% endcall %}
    {% endif %}

    {{ return(none) }}

{% endmacro %}
