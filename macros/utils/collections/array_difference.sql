{# array_difference([1, 2, 3, 4, 5], [0, 1, 2], [1, 2, 3]) == [4, 5] #}

{% macro array_difference(A) %}

    {% set result = [] %}

    {% for x in A %}
        {% if x not in result %}
            {% for B in varargs if B is iterable and x in B %}
                {# no op #}
            {% else %}
                {% do result.append(x) %}
            {% endfor %}
        {% endif %}
    {% endfor %}

    {{ return(result) }}

{% endmacro %}
