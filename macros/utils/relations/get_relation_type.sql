{% macro get_relation_type(relation, type=none, query=none) %}
    {{ return(adapter.dispatch('get_relation_type')(relation, type, query)) }}
{% endmacro %}

{% macro default__get_relation_type(relation, type) %}
    {{ return({'type': relation.type}) }}
{% endmacro %}

{%- macro _show_relation(relation, type, query=none) -%}

    {{ return(result) }}
{%- endmacro -%}

{% macro snowflake__get_relation_type(relation, type, query=none) %}
    {% set relation = get_fully_qualified_relation(relation) %}

    {% if execute %}
        {% set rows = [] %}
        {% set show_type = type %}

        {% if type == 'function' %}
            {% set show_type = 'user function' %}
        {% elif type == 'procedure' %}
            {% set show_type = 'user procedure' %}
        {% endif %}

        {% if type is not none %}

            {% if relation.identifier is not none %}
                {% set identifier = relation.identifier %}
            {% elif relation.schema is not none %}
                {% set identifier = relation.schema %}
            {% else %}
                {% set identifier = relation.database %}
            {% endif %}

            {% set show_query | replace('\n                ', '\n') | trim %}
                {{ show_relation(relation, type) }}
                {%- if type in ['function', 'user function', 'procedure', 'user procedure'] %}
                ->> select *, regexp_replace(right("arguments", len("arguments") - len("name")), '[)] RETURN(.|\s)*', ')') as "arguments_no_return" from $1 where "name" = '{{ escape_ansii(identifier) }}'
                {%- else %}
                ->> select * from $1 where "name" = '{{ escape_ansii(identifier) }}'
                {%- endif %}
                {%- if query is not none %}
                ->> {{ query }}
                {%- endif %}
            {% endset %}

            {% for row in run_query(show_query) %}
                {% do rows.append(row) %}
            {% endfor %}
        {% endif %}

        {% for row in rows %}
            {{ return({'type': type, 'rows': rows}) }}
        {% endfor %}

        {% set try_query | replace('\n            ', '\n') %}
            begin
                let res resultset := (
                    select
                        reduce(
                            split(
                                regexp_replace(
                                    lower(get_ddl('table', '{{ relation }}')),
                                    '^(\s|\n)*(create )?(or )?(replace )?(alter )?(secure )?(local )?(global )?(temp )?(temporary )?(volatile )?(transient )?(recursive )?',
                                    ''
                                ),
                                ' '
                            ),
                            '',
                            (result, word) -> iff(result like any ('%table', '%view'), result, result || ' ' || word)
                        ) as relation_type
                );
                return table(res);
            exception when other then
                let error_response resultset := (select 1 as x where false);
                return table(error_response);
            end
        {% endset %}

        {% set execute_query | trim %}
            execute immediate '{{ escape_ansii(try_query) }}'
        {% endset %}

        {% for row in run_query(execute_query) %}
            {{ return({'type': row[0]}) }}
        {% endfor %}

        {% for check_type in [
            'data metric function',
            'user function',
            'user procedure',
            'task',
            'stream',
            'stage',
            'pipe',
            'file format',
            'alert',
            'row access policy',
            'masking policy',
            'tag',
            'aggregation policy',
            'join policy',
            'privacy policy',
            'projection policy',
            'sequence',
            'secret',
            'network rule',
            'git repository',
            'authentication policy',
            'password policy',
            'session policy',
            'streamlit',
            'notebook',
            'image repository',
            'semantic view'
        ] if check_type != show_type %}
            {% for row in run_query(show_relation(relation, check_type)) %}
                {% do rows.append(row) %}
            {% endfor %}

            {% for row in rows %}
                {{ return({
                    'type':
                        check_type
                            .replace('user function', 'function')
                            .replace('user procedure', 'procedure'),
                    'rows': rows
                }) }}
            {% endfor %}

        {% endfor %}
    {% endif %}

    {{ return({'type': none}) }}

{% endmacro %}
