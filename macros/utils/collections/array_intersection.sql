{# array_intersection([1, 2, 3, 4, 5], [0, 1, 2], [1, 2, 3]) == [1, 2] #}

{% macro array_intersection(A) %}

    {% set result = [] %}

    {% if A is iterable %}
        {% for x in A %}
            {% if x not in result %}
                {% for B in varargs if B is iterable and x not in B %}
                    {# no op #}
                {% else %}
                    {% do result.append(x) %}
                {% endfor %}
            {% endif %}
        {% endfor %}
    {% endif %}

    {{ return(result) }}

{% endmacro %}
