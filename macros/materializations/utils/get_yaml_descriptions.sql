{% macro get_yaml_descriptions(model) %}

    {% set descriptions = {} %}

    {% if model.columns %}
        {% for k, v in model.columns.items()
            if v.get('description') is not none %}
                {% do descriptions.update({get_fully_qualified_identifier(k): v.get('description')}) %}
        {% endfor %}
    {% endif %}

    {{ return(descriptions) }}

{% endmacro %}
