{% macro get_relation_type(relation, type=none) %}
    {{ return(adapter.dispatch('get_relation_type')(relation, type)) }}
{% endmacro %}

{% macro default__get_relation_type(relation, type) %}
    {{ return({'type': relation.type}) }}
{% endmacro %}

{% macro snowflake__get_relation_type(relation, type) %}
    {% set relation = get_fully_qualified_relation(relation) %}

    {% set query %}
        select
            (
                case is_dynamic when 'YES' then 'dynamic ' else '' end
                || case is_iceberg when 'YES' then 'iceberg ' else '' end
                || case table_type
                        when 'VIEW' then 'view'
                        when 'MATERIALIZED VIEW' then 'materialized view'
                        when 'EXTERNAL TABLE' then 'external table'
                        when 'EVENT TABLE' then 'event table'
                        else 'table'
                    end
            ) as materialization
        from
            {{ relation.database }}.information_schema.tables
        where
            table_schema ilike $${{ relation.schema }}$$
            and table_name ilike $${{ relation.identifier }}$$

        {%- if type != 'function' %}
        union all

        select
            'function' as materialization
        from
            {{ relation.database }}.information_schema.functions
        where
            function_schema ilike $${{ relation.schema }}$$
            and function_name ilike $${{ relation.identifier }}$$
        {%- endif %}

        {%- if type != 'procedure' %}
        union all

        select
            'procedure' as materialization
        from
            {{ relation.database }}.information_schema.procedures
        where
            procedure_schema ilike $${{ relation.schema }}$$
            and procedure_name ilike $${{ relation.identifier }}$$
        {%- endif %}
    {% endset %}

    {% if execute %}
        {% if type is not none %}
            {% set rows = [] %}
            {% if type == 'function' %}
                {% for row in run_query(show_relation(relation, 'user function')) if row['name'] == relation.identifier %}
                    {% do rows.append(row) %}
                {% endfor %}
            {% else %}
                {% for row in run_query(show_relation(relation, type)) if row['name'] == relation.identifier %}
                    {% do rows.append(row) %}
                {% endfor %}
            {% endif %}
            {% for row in rows if relation['name'] == relation.identifier %}
                {{ return({'type': type, 'rows': rows}) }}
            {% endfor %}
        {% endif %}

        {% set rows = run_query(query) %}
        {% for row in rows %}
            {{ return({'type': row['MATERIALIZATION'], 'rows': rows}) }}
        {% endfor %}

        {% if type != 'stream' %}
            {% set rows = [] %}
            {% for row in run_query(show_relation(relation, 'stream')) if row['name'] == relation.identifier %}
                {% do rows.append(row) %}
            {% endfor %}
            {% for row in rows %}
                {{ return({'type': 'stream', 'rows': rows}) }}
            {% endfor %}
        {% endif %}
    {% endif %}

    {{ return({'type': none}) }}

{% endmacro %}
