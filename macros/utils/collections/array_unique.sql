{# array_unique([1, 1, 2, 3, 2]) == [1, 2, 3] #}

{% macro array_unique(A) %}

    {% set result = [] %}

    {% for x in A %}
        {% if x not in result %}
            {% do result.append(x) %}
        {% endif %}
    {% endfor %}

    {{ return(result) }}

{% endmacro %}
