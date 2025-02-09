{% macro array_difference(A, B) %}

    {% set result = [] %}

    {% for x in A %}
        {% if x not in result and x not in B %}
            {% do result.append(x) %}
        {% endif %}
    {% endfor %}

    {{ return(result) }}

{% endmacro %}
