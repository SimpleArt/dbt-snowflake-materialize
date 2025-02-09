{% macro parse_arguments(code) %}
    {% set code = code.strip() %}

    {% if '(' not in code or not code.endswith(')') %}
        {{ return(none) }}
    {% endif %}

    {% set i = code.index('(') %}
    {% set name = code[:i].strip() %}

    {% set arguments = [] %}
    {% set state = {'temp': '', 'kwarg': none, 'closed': false} %}
    {% set quote = '"' ~ "'" %}
    {% set space = ' \n' %}

    {% for c in code[i + 1:][:-1].strip() %}

        {% if (state['temp'] == '' or state['closed']) and c in space %}
            {% do state.update({}) %}

        {% elif state['temp'] == '' %}
            {% if not ((state['kwarg'] is none and c == '_' or c.isalpha()) or c in quote or c.isnumeric()) %}
                {{ return(none) }}
            {% endif %}

            {% do state.update({'temp': c}) %}

        {% elif not state['closed'] and state['temp'][0] in quote %}
            {% if state['temp'].startswith(c) %}
                {% do state.update({'temp': state['temp'] ~ c, 'closed': true}) %}
            {% else %}
                {% do state.update({'temp': state['temp'] ~ c}) %}
            {% endif %}

        {% elif not state['closed'] and state['temp'][0].isnumeric() %}
            {% if c in space %}
                {% do state.update({'closed': true}) %}

            {% elif c.isnumeric() %}
                {% do state.update({'temp': state['temp'] ~ c}) %}

            {% elif c == '.' and '.' in state['temp'] %}
                {{ return(none) }}

            {% elif c == '.' %}
                {% do state.update({'temp': state['temp'] ~ c}) %}

            {% elif c == '=' or c != ',' %}
                {{ return(none) }}

            {% elif state['kwarg'] is none %}
                {% if '.' in state['temp'] %}
                    {% do arguments.append(state['temp'] | float) %}
                {% else %}
                    {% do arguments.append(state['temp'] | int) %}
                {% endif %}

                {% do state.update({'temp': ''}) %}

            {% else %}
                {% if '.' in state['temp'] %}
                    {% do arguments.append({state['kwarg']: state['temp'] | float}) %}
                {% else %}
                    {% do arguments.append({state['kwarg']: state['temp'] | int}) %}
                {% endif %}

                {% do state.update({'temp': '', 'kwarg': none}) %}

            {% endif %}

        {% elif not state['closed'] %}
            {% if c in space %}
                {% do state.update({'closed': true}) %}
            {% elif c == '=' and state['kwarg'] is none %}
                {% do state.update({'temp': '', 'kwarg': state['kwarg']}) %}
            {% elif c == '=' %}
                {{ return(none) }}
            {% elif c == '_' or c.isalnum() %}
                {% do state.update({'temp': state['temp'] ~ c}) %}
            {% else %}
                {{ return(none) }}
            {% endif %}

        {% elif c not in (space ~ '=,') %}
            {{ return(none) }}

        {% elif c == '=' and (state['temp'][0] == '_' or state['temp'][0].isalpha()) %}
            {% do state.update({'temp': '', 'kwarg': state['temp'], 'closed': false}) %}

        {% elif c == '=' %}
            {{ return(none) }}

        {% elif c == ',' and state['temp'][0] in quote %}
            {% if state['kwarg'] is none %}
                {% do arguments.append(state['temp'].strip(state['temp'][0])) %}
                {% do state.update({'temp': '', 'closed': false}) %}
            {% else %}
                {% do arguments.append({state['kwarg']: state['temp'].strip(state['temp'][0])}) %}
                {% do state.update({'temp': '', 'closed': false, 'kwarg': none}) %}
            {% endif %}

        {% elif c == ',' and state['temp'][0].isnumeric() %}
            {% if state['kwarg'] is none %}
                {% do arguments.append(state['temp'] | int) %}
                {% do state.update({'temp': '', 'closed': false}) %}
            {% else %}
                {% do arguments.append({state['kwarg']: state['temp'] | int}) %}
                {% do state.update({'temp': '', 'closed': false, 'kwarg': none}) %}
            {% endif %}

        {% elif c == ',' %}
            {{ return(none) }}

        {% endif %}
    {% endfor %}

    {% if state['closed'] and state['temp'][0] in quote %}
        {% if state['kwarg'] is none %}
            {% do arguments.append(state['temp'].strip(state['temp'][0])) %}
        {% else %}
            {% do arguments.append({state['kwarg']: state['temp'].strip(state['temp'][0])}) %}
        {% endif %}

    {% elif state['closed'] and state['temp'][0].isnumeric() %}
        {% if state['kwarg'] is none %}
            {% do arguments.append(state['temp'] | int) %}
        {% else %}
            {% do arguments.append({state['kwarg']: state['temp'] | int}) %}
        {% endif %}

    {% else %}
        {{ return(none) }}

    {% endif %}

    {{ return({'name': name, 'arguments': arguments}) }}
{% endmacro %}

{% macro parse_ref(jinja) %}
    {% set parsed = parse_arguments(jinja) %}

    {% if parsed is none %}
        {{ return('<COULD NOT PARSE: ' ~ jinja ~ '>') }}
    {% endif %}

    {% set arguments = parsed['arguments'] %}

    {% if parsed['name'] == 'ref' %}
        {% if arguments | length == 1 and arguments[0] is string %}
            {{ return(ref(arguments[0])) }}
        {% elif arguments | length == 2 and arguments[0] is string and arguments[1] is mapping and 'v' in arguments[1] and arguments[1]['v'] is number %}
            {{ return(ref(arguments[0], v=arguments[1]['v'])) }}
        {% endif %}
    {% elif parsed['name'] == 'source' and arguments | length == 2 and arguments[0] is string and arguments[1] is string %}
        {{ return(source(arguments[0], arguments[1])) }}
    {% endif %}

    {{ return('<COULD NOT PARSE: ' ~ jinja ~ '>') }}
{% endmacro %}

{% macro parse_jinja(code) %}
    {% set relations = [] %}
    {% set state = {'code': code, 'in_jinja': false, 'prev': '', 'jinja': ''} %}

    {% for c in code %}
        {% if state['in_jinja'] and c == '}' and state['prev'] == '}' %}
            {% set jinja = state['jinja'] ~ '}' %}

            {% set reference = parse_ref(jinja[2:-2]) %}
            {% set relation = string_to_relation(reference | string) %}

            {% if relation is not none %}
                {% do relations.append(relation) %}
            {% endif %}

            {% do state.update({
                'code': state['code'].replace(jinja, reference | string),
                'in_jinja': false,
                'jinja': ''
            }) %}

        {% elif state['in_jinja'] %}
            {% do state.update({'jinja': state['jinja'] ~ c}) %}

        {% elif c == '{' and state['prev'] == '{'%}
            {% do state.update({
                'in_jinja': true,
                'jinja': '{{'
            }) %}

        {% endif %}

        {% do state.update({'prev': c}) %}
    {% endfor %}

    {{ return({'code': state['code'], 'relations': relations}) }}

{% endmacro %}
