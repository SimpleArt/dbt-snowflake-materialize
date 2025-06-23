{% macro remove_select(query) %}
    {% set state = {'flag': 0, 'nesting': 0, 'prev1': none, 'prev2': none, 'prev3': none, 'prev4': none, 'prev5': none, 'result': []} %}

    {% for c in query %}
        {% if state['flag'] == 0 and c == '-' and state['prev1'] == '-' %}
            {% do state.update({'flag': 1}) %}
        {% elif state['flag'] == 0 and c == '*' and state['prev1'] == '/' %}
            {% do state.update({'flag': 2}) %}
        {% elif state['flag'] == 1 and c in ('\n', '\r') %}
            {% do state.update({'flag': 0}) %}
        {% elif state['flag'] == 2 and c == '/' and state['prev1'] == '*' %}
            {% do state.update({'flag': 0}) %}
        {% elif state['flag'] == 0 %}
            {% if c == '(' %}
                {% do state.update({'nesting': state['nesting'] + 1}) %}
            {% elif c == ')' %}
                {% do state.update({'nesting': state['nesting'] - 1}) %}
            {% elif state['nesting'] == 0 and state['prev5'] == 's' and state['prev4'] == 'e' and state['prev3'] == 'l' and state['prev2'] == 'e' and state['prev1'] == 'c' and c == 't' %}
                {{ return(state['result'][:-5] | join('')) }}
            {% endif %}
        {% endif %}

        {% do state.update({'prev5': state['prev4']}) %}
        {% do state.update({'prev4': state['prev3']}) %}
        {% do state.update({'prev3': state['prev2']}) %}
        {% do state.update({'prev2': state['prev1']}) %}
        {% do state.update({'prev1': c}) %}
        {% do state['result'].append(c) %}
    {% endfor %}

    {{ return(query) }}
{% endmacro %}
