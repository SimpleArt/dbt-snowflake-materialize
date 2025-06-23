{% macro _config_recursive(obj) %}
    {% if obj is string or obj is not iterable %}
        {{ return(obj) }}
    {% elif obj is mapping %}
        {% set result = {} %}
        {% for k, v in obj.items() %}
            {% do result.update({k: _config_recursive(v)}) %}
        {% endfor %}
        {{ return(result) }}
    {% endif %}

    {% set result = [] %}

    {% for x in obj %}
        {% do result.append(_config_recursive(x)) %}
    {% endfor %}

    {% set c = obj | string %}

    {% if c.startswith('[') %}
        {% set result = {'list': result} %}
    {% elif c.startswith('s') or c.startswith('{') %}
        {% set result = {'set': result} %}
    {% elif c.startswith('(') %}
        {% set result = {'tuple': result} %}
    {% else %}
        {% set result = {'iterator': result} %}
    {% endif %}

    {{ return(result) }}
{% endmacro %}

{% macro config_recursive() %}
    {% set result = [] %}

    {% for obj in varargs %}
        {% do result.append(_config_recursive(obj))%}
    {% endfor %}

    {{ return(result) }}
{% endmacro %}
