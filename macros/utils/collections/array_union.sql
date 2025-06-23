{# array_union([1, 2, 3, 4, 5], [0, 1, 2], [1, 2, 3]) == [1, 2, 3, 4, 5, 0] #}

{% macro array_union() %}

    {% set result = [] %}

    {% for A in varargs if A is iterable %}
        {% for x in A %}
            {% if x not in result %}
                {% do result.append(x) %}
            {% endif %}
        {% endfor %}
    {% endfor %}

    {{ return(result) }}

{% endmacro %}
