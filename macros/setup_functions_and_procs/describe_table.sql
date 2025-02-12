{% macro describe_table(relation=none) %}
    {% set proc_relation = api.Relation.create(
        database=target.database,
        schema=target.schema,
        identifier="DESCRIBE_TABLE"
    ) %}
    {% set proc_relation = get_fully_qualified_relation(proc_relation) %}
    {% if relation is none %}
        {{ return(proc_relation) }}
    {% else %}
        {{ return(proc_relation ~ '($$' ~ relation ~ '$$)') }}
    {% endif %}
{% endmacro %}

{%- macro create_describe_table() -%}
{%- set target_relation = describe_table() -%}
{%- set temp_relation = target_relation.incorporate(
    path={'identifier': "DESCRIBE_TABLE__DBT_TEMP"}
) -%}

with create_describe_table as procedure()
    returns table()
    language python
    runtime_version = 3.11
    packages = ('snowflake-snowpark-python')
    handler = 'create_describe_table'
as $$
def create_describe_table(session):
    session.sql(
        "create or replace temporary table {{ temp_relation }}(x int)"
    ).collect()
    job = session.sql(
        "describe table {{ temp_relation }}"
    ).collect_nowait()
    job.result()
    session.sql(
        f"create or replace temporary table {{ temp_relation }} as\n"
        f"    select * from table(result_scan('{job.query_id}'))"
    ).collect()
    job = session.sql(
        "describe table {{ temp_relation }}"
    ).collect_nowait()
    job.result()
    returns = session.sql({{ get_full_table_schema(temp_relation, "result_scan('{job.query_id}')") }}).collect()[0][0]
    session.sql("drop table if exists {{ temp_relation }}").collect_nowait()
    return session.sql(
        f"create or replace procedure {{ target_relation }}(table_name varchar)\n"
        f"    returns table({returns})\n"
        f"as {chr(36)}{chr(36)}\n"
        f"    begin\n"
        f"        let res resultset := (describe table identifier(:table_name)){chr(59)}\n"
        f"        return table(res){chr(59)}\n"
        f"    end\n"
        f"{chr(36)}{chr(36)}"
    )
$$

call create_describe_table()
{%- endmacro -%}

{% macro setup_describe_table() %}
    {% if load_result("__SETUP_DESCRIBE_TABLE__") is none %}
        {% call statement("__SETUP_DESCRIBE_TABLE__") %}
            {{- create_describe_table() -}}
        {% endcall %}
    {% endif %}

    {{ return("select 1 as no_op") }}
{% endmacro %}
