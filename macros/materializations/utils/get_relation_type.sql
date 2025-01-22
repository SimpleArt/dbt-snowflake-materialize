{% macro get_relation_type(relation) %}

{% set query -%}
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
    {{ adapter.quote(relation.database) }}.information_schema.tables
where
    table_schema = $${{ relation.schema }}$$
    and table_name = $${{ relation.identifier }}$$

union all

select
    'function' as materialization
from
    {{ adapter.quote(relation.database) }}.information_schema.functions
where
    function_schema = $${{ relation.schema }}$$
    and function_name = $${{ relation.identifier }}$$

union all

select
    'procedure' as materialization
from
    {{ adapter.quote(relation.database) }}.information_schema.procedures
where
    function_schema = $${{ relation.schema }}$$
    and function_name = $${{ relation.identifier }}$$
{%- endset %}

    {% if execute %}
        {% for row in run_query(query) %}
            {{ return(row['MATERIALIZATION']) }}
        {% endfor %}
    {% endif %}

    {{ return(none) }}

{% endmacro %}
