{% macro array_union(A, B) %}

    {% set result = [] %}

    {% for x in A %}
        {% if x not in result %}
            {% do result.append(x) %}
        {% endif %}
    {% endfor %}

    {% for x in B %}
        {% if x not in result %}
            {% do result.append(x) %}
        {% endif %}
    {% endfor %}

    {{ return(result) }}

{% endmacro %}
