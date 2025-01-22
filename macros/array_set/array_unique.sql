{% macro array_unique(A) %}

    {% set result = [] %}

    {% for x in A %}
        {% if x not in result %}
            {% do result.append(x) %}
        {% endif %}
    {% endfor %}

    {{ return(result) }}

{% endmacro %}
