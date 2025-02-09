{% macro get_relation_type(relation) %}
    {{ return(adapter.dispatch('get_relation_type')(relation)) }}
{% endmacro %}

{% macro default__get_relation_type(relation) %}
    {{ return({'type': relation.type}) }}
{% endmacro %}

{% macro snowflake__get_relation_type(relation) %}
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

        union all

        select
            'function' as materialization
        from
            {{ relation.database }}.information_schema.functions
        where
            function_schema ilike $${{ relation.schema }}$$
            and function_name ilike $${{ relation.identifier }}$$

        union all

        select
            'procedure' as materialization
        from
            {{ relation.database }}.information_schema.procedures
        where
            procedure_schema ilike $${{ relation.schema }}$$
            and procedure_name ilike $${{ relation.identifier }}$$
    {% endset %}

    {% if execute %}
        {% for row in run_query(query) %}
            {{ return({'type': row['MATERIALIZATION'], 'row': row}) }}
        {% endfor %}

        {% for row in run_query(show_relation(relation, 'stream')) %}
            {{ return({'type': 'stream', 'row': row}) }}
        {% endfor %}
    {% endif %}

    {{ return({'type': none}) }}

{% endmacro %}
