{% macro drop_relation_unless(relation, drop_unless_type=none, metadata=none, transient=none, alter_if=none) %}

    {% set result = get_relation_type(relation, drop_unless_type) %}
    {% set type = result.get('type') %}
    {% set rows = result.get('rows') %}

    {% if drop_unless_type is none %}
        {% set drop_unless_type = relation.type %}
    {% endif %}

    {% if type is none or not execute %}
        {# If the object doesn't already exist, run create or replace, #}
        {# which may include additional parameters, such as "create or replace secure ...", #}
        {# which ensures additional "alter" queries do not need to be ran. #}
        {{ return({'DDL': 'create or replace'}) }}

    {% elif type in ['function', 'procedure'] %}
        {# If a function/procedure needs to be created, first check if it's already created. #}
        {% if type == drop_unless_type and not should_full_refresh() %}
            {% for row in rows %}
                {% set comment = row.get('comment', row.get('description', '')) %}
                {% set state = {'flag': true} %}

                {% for part in metadata if part not in comment %}
                    {% do state.update({'flag': false}) %}
                {% endfor %}

                {% if state['flag'] %}
                    {{ return({'DDL': 'create if not exists'}) }}
                {% endif %}
            {% endfor %}

        {# If should not be a function, drop all functions. #}
        {% elif type == 'function' %}
            {% for row in run_query(show_relation(relation, 'user function')) %}
                {% call statement('drop_callable') %}
                    drop function if exists {{ relation }}({{ row['arguments'][(relation.identifier|length + 1):].split(') RETURN ')[0] }})
                {% endcall %}
            {% endfor %}

        {# If should not be a procedure, drop all procedures. #}
        {% else %}
            {% for row in run_query(show_relation(relation, 'procedure')) %}
                {% call statement('drop_callable') %}
                    drop procedure if exists {{ relation }}({{ row['arguments'][(relation.identifier|length + 1):].split(') RETURN ')[0] }})
                {% endcall %}
            {% endfor %}

        {% endif %}

        {{ return({'DDL': 'create or replace'}) }}

    {% elif type != drop_unless_type %}
        {# If the object exists of the wrong type, then drop it. #}
        {% call statement('drop_object') %}
            drop {{ type }} if exists {{ relation }}
        {% endcall %}

        {{ return({'DDL': 'create or replace'}) }}

    {% elif should_full_refresh() %}
        {{ return({'DDL': 'create or replace'}) }}

    {% endif %}

    {# Check if the existing object matches the expected object. #}
    {% for row in rows %}
        {% set text = row.get('text') %}
        {% set comment = row.get('comment', row.get('description')) %}

        {% if row.get('stale') == 'true' or row.get('scheduling_state') == 'SUSPENDED' %}
            {{ return({'DDL': 'create or replace'}) }}
        {% elif transient is not none %}
            {% if transient and row.get('kind') != 'TRANSIENT' %}
                {{ return({'DDL': 'create or replace'}) }}
            {% elif not transient and row.get('kind') == 'TRANSIENT' %}
                {{ return({'DDL': 'create or replace'}) }}
            {% endif %}
        {% endif %}

        {% if metadata is not none %}
            {% for part in metadata if (not text or part not in text) and (not comment or part not in comment) %}
                {{ return({'DDL': 'create or replace'}) }}
            {% endfor %}
        {% endif %}

        {% if alter_if is not none %}
            {% set result = {'DDL': 'alter if exists', 'alter_if': []} %}
            {% for part in alter_if if (not text or part not in text) and (not comment or part not in comment) %}
                {% do result['alter_if'].append(part) %}
            {% endfor %}
            {% if result['alter_if'] != [] %}
                {{ return(result) }}
            {% endif %}
        {% endif %}
    {% endfor %}

    {# Check if the existing object can still be queried. #}
    {% if type in ['row access policy'] or is_queryable(relation) %}
        {{ return({'DDL': 'create if not exists'}) }}
    {% else %}
        {{ return({'DDL': 'create or replace'}) }}
    {% endif %}

{% endmacro %}
