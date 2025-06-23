{% macro compile_config(obj, stringify=false) %}
    {% if obj is string or obj is not iterable %}
        {% set result = obj %}
    {% elif obj is not mapping %}
        {% set result = obj %}
        {% if obj is iterable and obj is not string %}
            {% set temp = [] %}
            {% for x in obj %}
                {% do temp.append(compile_config(x, stringify=stringify) | string) %}
            {% endfor %}
            {{ return('[' ~ temp | join(', ') ~ ']') }}
        {% endif %}
    {% elif 'dict' in obj %}
        {% set result = [] %}
        {% for k, v in obj.get('dict') | dictsort %}
            {% do result.append(compile_config(k, stringify=none) ~ ': ' ~ compile_config(v, stringify=none)) %}
        {% endfor %}
        {{ return('{' ~ result | join(', ') ~ '}') }}
    {% elif 'iterator' in obj or 'list' in obj or 'set' in obj or 'tuple' in obj %}
        {% set result = [] %}
        {% for x in obj.get('iterator', obj.get('list', obj.get('set', obj.get('tuple')))) %}
            {% do result.append(compile_config(x, stringify=stringiyf)) %}
        {% endfor %}
        {% set result = result | join(', ') %}
        {% if 'list' in obj %}
            {% set result = '[' ~ result ~ ']' %}
        {% elif 'set' in obj %}
            {% set result = '{' ~ result ~ '}' %}
        {% elif 'tuple' in obj %}
            {% set result = '(' ~ result ~ ')' %}
        {% endif %}
        {{ return(result) }}
    {% elif 'string' in obj %}
        {{ return(compile_config(obj.get('string'), stringify=true)) }}
    {% elif 'source' in obj and 'table' in obj %}
        {% set result = source(obj.get('source'), obj.get('table')) %}
        {% if 'file' in obj %}
            {% set result = (result | string) ~ '/' ~ obj.get('file') %}
        {% endif %}
    {% elif 'model' in obj %}
        {% if 'package' in obj and 'version' in obj %}
            {% set result = ref(obj.get('package'), obj.get('model'), v=obj.get('version')) %}
        {% elif 'package' in obj %}
            {% set result = ref(obj.get('package'), obj.get('model')) %}
        {% elif 'version' in obj %}
            {% set result = ref(obj.get('model'), v=obj.get('version')) %}
        {% else %}
            {% set result = ref(obj.get('model')) %}
        {% endif %}
        {% if 'file' in obj %}
            {% set result = (result | string) ~ '/' ~ obj.get('file') %}
        {% endif %}
    {% else %}
        {% set result = [] %}
        {% for k, v in obj | dictsort %}
            {% do result.append(compile_config(k) ~ ' = ' ~ compile_config(v, stringify=stringify)) %}
        {% endfor %}
        {{ return(result | join(', ')) }}
    {% endif %}

    {% if stringify or (obj is string and stringify is none) %}
        {{ return("'" ~ escape_ansii(result | string) ~ "'") }}
    {% else %}
        {{ return(obj) }}
    {% endif %}
{% endmacro %}
