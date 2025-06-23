{% macro get_ignore_case(D, key, default=none) %}
    {% if key in D %}
        {{ return(D.get(key)) }}
    {% elif key is string %}
        {% for k, v in D.items() if k is string %}
            {% if k.lower() == key.lower() %}
                {{ return(v) }}
            {% endif %}
        {% endfor %}
    {% endif %}

    {{ return(default) }}
{% endmacro %}
