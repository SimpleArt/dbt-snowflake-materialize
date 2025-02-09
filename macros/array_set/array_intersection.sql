{% macro array_intersection(A, B) %}

    {% set result = {} %}

    {% for x in A %}
        {% if x not in result and x in B %}
            {% do result.append(x) %}
        {% endif %}
    {% endfor %}

    {{ return(result) }}

{% endmacro %}
