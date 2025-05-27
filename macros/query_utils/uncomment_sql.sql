{% macro uncomment_sql(sql) %}
    {% set state = {'flag': 0, 'prev': none, 'result': []} %}
    {% for c in sql %}
        {% if state['flag'] == 0 and c == '-' and state['prev'] == '-' %}
            {% do state.update({'flag': 1, 'result': state['result'][:-1]}) %}
        {% elif state['flag'] == 0 and c == '*' and state['prev'] == '/' %}
            {% do state.update({'flag': 2, 'result': state['result'][:-1]}) %}
        {% elif state['flag'] == 1 and c == '\n' %}
            {% do state.update({'flag': 0}) %}
        {% elif state['flag'] == 2 and c == '/' and state['prev'] = '*' %}
            {% do state.update({'flag': 0}) %}
        {% else %}
            {% do state['result'].append(c) %}
        {% endif %}
        {% do state.update({'prev': c})
    {% endfor %}
    {{ return(result | join('')) }}
{% endmacro %}
