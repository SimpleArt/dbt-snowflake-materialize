{% macro get_relation_type(relation, type=none) %}
    {{ return(adapter.dispatch('get_relation_type')(relation, type)) }}
{% endmacro %}

{% macro default__get_relation_type(relation, type) %}
    {{ return({'type': relation.type}) }}
{% endmacro %}

{% macro snowflake__get_relation_type(relation, type) %}
    {% set relation = get_fully_qualified_relation(relation) %}

    {% if execute %}
        {% set rows = [] %}
        {% set show_type = type %}

        {% if type == 'function' %}
            {% set show_type = 'user function' %}
        {% endif %}

        {% if type is not none %}
            {%
                for row in run_query(show_relation(relation, show_type))
                if row['name'] == relation.identifier
            %}
                {% do rows.append(row) %}
            {% endfor %}
        {% endif %}

        {% for row in rows %}
            {{ return({'type': type, 'rows': rows}) }}
        {% endfor %}

        {% set query %}
            with get_relation_type as procedure()
                returns table()
            as '
                begin
                    let res resultset := (
                        select
                            reduce(
                                split(
                                    regexp_replace(
                                        lower(get_ddl($$table$$, $${{ relation }}$$)),
                                        $$^(\s|\n)*(create )?(or )?(replace )?(alter )?(secure )?(local )?(global )?(temp )?(temporary )?(volatile )?(transient )?(recursive )?$$,
                                        $$$$
                                    ),
                                    $$ $$
                                ),
                                $$$$,
                                (result, word) -> iff(result like any ($$%table$$, $$%view$$), result, result || $$ $$ || word)
                            ) as relation_type
                    );
                    return table(res);
                exception when other then
                    let error_response resultset := (select 1 as x where false);
                    return table(error_response);
                end
            '

            call get_relation_type()
        {% endset %}

        {% for row in run_query(query) %}
            {{ return({'type': row[0]}) }}
        {% endfor %}

        {% for check_type in [
            'data metric function',
            'user function',
            'procedure',
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
            {%
                for row in run_query(show_relation(relation, check_type))
                if row['name'] == relation.identifier
            %}
                {% do rows.append(row) %}
            {% endfor %}

            {% for row in rows %}
                {{ return({'type': check_type.replace('user function', 'function'), 'rows': rows}) }}
            {% endfor %}

        {% endfor %}
    {% endif %}

    {{ return({'type': none}) }}

{% endmacro %}
