{% macro config_ref() %}
    {% set result = {} %}

    {% if 'version' in kwargs %}
        {% do result.update({'version': kwargs.get('version')}) %}
    {% elif 'v' in kwargs %}
        {% do result.update({'version': kwargs.get('v')}) %}
    {% endif %}

    {% if (varargs | length) == 1 %}
        {% do result.update({'model': varargs[0]}) %}
    {% else %}
        {% do result.update({'package': varargs[0], 'model': varargs[1]}) %}
    {% endif %}

    {% if 'package' in result and 'version' in result %}
        {% do ref(result['package'], result['model'], v=result['version']) %}
    {% elif 'package' in result %}
        {% do ref(result['package'], result['model']) %}
    {% elif 'package' in result %}
        {% do ref(result['model'], v=result['version']) %}
    {% elif 'package' in result %}
        {% do ref(result['model']) %}
    {% endif %}

    {% if kwargs.get('file') is string %}
        {% do result.update({'file': kwargs.get('file')}) %}
    {% endif %}

    {% if kwargs.get('string') %}
        {% set result = {'string': result} %}
    {% endif %}

    {{ return(result) }}
{% endmacro %}
