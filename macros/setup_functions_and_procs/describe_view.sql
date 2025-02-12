{% macro describe_view(relation=none) %}
    {% set proc_relation = api.Relation.create(
        database=target.database,
        schema=target.schema,
        identifier="DESCRIBE_VIEW"
    ) %}
    {% set proc_relation = get_fully_qualified_relation(proc_relation) %}
    {% if relation is none %}
        {{ return(proc_relation) }}
    {% else %}
        {{ return(proc_relation ~ '($$' ~ relation ~ '$$)') }}
    {% endif %}
{% endmacro %}

{%- macro create_describe_view() -%}
{%- set target_relation = describe_view() -%}
{%- set temp_relation = target_relation.incorporate(
    path={'identifier': "DESCRIBE_VIEW__DBT_TEMP"}
) -%}
{%- set describe_relation = target_relation.incorporate(
    path={'identifier': "DESCRIBE_VIEW__DBT_TEMP__DBT_TEMP"}
) -%}

with create_describe_view as procedure()
    returns table()
    language python
    runtime_version = 3.11
    packages = ('snowflake-snowpark-python')
    handler = 'create_describe_view'
as $$
def create_describe_view(session):
    session.sql(
        "create or replace temporary view {{ temp_relation }} as select 1 as x"
    ).collect()
    {{ get_full_query_schema('describe view ' ~ (temp_relation | string), describe_relation) }}
    session.sql("drop view if exists {{ temp_relation }}").collect_nowait()
    return session.sql(
        f"create or replace procedure {{ target_relation }}(view_name varchar)\n"
        f"    returns table({returns})\n"
        f"as {chr(36)}{chr(36)}\n"
        f"    begin\n"
        f"        let res resultset := (describe view identifier(:view_name)){chr(59)}\n"
        f"        return table(res){chr(59)}\n"
        f"    end\n"
        f"{chr(36)}{chr(36)}"
    )
$$

call create_describe_view()
{%- endmacro -%}

{% macro setup_describe_view() %}
    {% if load_result("__SETUP_DESCRIBE_VIEW__") is none %}
        {% call statement("__SETUP_DESCRIBE_VIEW__") %}
            {{- create_describe_view() -}}
        {% endcall %}
    {% endif %}

    {{ return("select 1 as no_op") }}
{% endmacro %}
