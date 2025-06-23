{# has_keys({'x': 1, 'y': 2}, 'x', 'y') == true #}
{# has_keys({'x': 1, 'y': 2}, 'x', 'y', 'z') == false #}

{% macro has_keys(D) %}
    {% if D is not mapping %}
        {{ return(false) }}
    {% endif %}

    {% for k in varargs if k not in D %}
        {{ return(false) }}
    {% endfor %}

    {{ return(true) }}
{% endmacro %}
