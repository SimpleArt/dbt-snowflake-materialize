{% macro show_streams(relation=none) %}
    {% set proc_relation = api.Relation.create(
        database=target.database,
        schema=target.schema,
        identifier="SHOW_STREAMS"
    ) %}
    {% set proc_relation = get_fully_qualified_relation(proc_relation) %}
    {% if relation is none %}
        {{ return(proc_relation) }}
    {% else %}
        {% set relation = get_fully_qualified_relation(string_to_relation(relation)) %}
        {{ return(
            (proc_relation | string) ~ '($$' ~ relation.identifier ~ '$$, $$' ~ relation.database ~ '.' ~ relation.schema ~ '$$)'
        ) }}
    {% endif %}
{% endmacro %}

{%- macro create_show_streams() -%}
{%- set target_relation = show_streams() -%}
{%- set temp_relation = target_relation.incorporate(
    path={'identifier': "SHOW_STREAMS__DBT_TEMP"}
) -%}

with create_show_streams as procedure()
    returns table()
    language python
    runtime_version = 3.11
    packages = ('snowflake-snowpark-python')
    handler = 'create_show_streams'
as $$
def create_show_streams(session):
    {{ get_full_query_schema("show streams limit 1", temp_relation) }}
    jobs = [
        session.sql(
            f"create or replace procedure {{ target_relation }}(table_name varchar)\n"
            f"    returns table({returns})\n"
            f"as {chr(36)}{chr(36)}\n"
            f"    begin\n"
            f"        let query varchar := 'show streams like {chr(92)}{chr(36)}{chr(92)}{chr(36)}' || :table_name || '{chr(92)}{chr(36)}{chr(92)}{chr(36)} in account'{chr(59)}
            f"        let res resultset := (execute immediate :query){chr(59)}\n"
            f"        return table(res){chr(59)}\n"
            f"    end\n"
            f"{chr(36)}{chr(36)}"
        ).collect_nowait(),
        session.sql(
            f"create or replace procedure {{ target_relation }}(table_name varchar, schema_name varchar)\n"
            f"    returns table({returns})\n"
            f"as {chr(36)}{chr(36)}\n"
            f"    begin\n"
            f"        let query varchar := 'show streams like {chr(92)}{chr(36)}{chr(92)}{chr(36)}' || :table_name || '{chr(92)}{chr(36)}{chr(92)}{chr(36)} in schema ' || :schema_name{chr(59)}
            f"        let res resultset := (execute immediate :query){chr(59)}\n"
            f"        return table(res){chr(59)}\n"
            f"    end\n"
            f"{chr(36)}{chr(36)}"
        ).collect_nowait(),
        session.sql(
            f"create or replace procedure {{ target_relation }}(table_name varchar, schema_name varchar, database_name varchar)\n"
            f"    returns table({returns})\n"
            f"as {chr(36)}{chr(36)}\n"
            f"    begin\n"
            f"        let query varchar := 'show streams like {chr(92)}{chr(36)}{chr(92)}{chr(36)}' || :table_name || '{chr(92)}{chr(36)}{chr(92)}{chr(36)} in schema ' || :database_name || '.' || :schema_name{chr(59)}
            f"        let res resultset := (execute immediate :query){chr(59)}\n"
            f"        return table(res){chr(59)}\n"
            f"    end\n"
            f"{chr(36)}{chr(36)}"
        ).collect_nowait(),
    ]
    for job in jobs:
        job.result()
    return session.sql(
        " union all ".join(
            f"select * from table(result_scan('{job.query_id}'))"
            for job in jobs
        )
    )
$$

call create_show_streams()
{%- endmacro -%}

{% macro setup_show_streams() %}
    {% if load_result("__SETUP_SHOW_STREAMS__") is none %}
        {% call statement("__SETUP_SHOW_STREAMS__") %}
            {{- create_show_streams() -}}
        {% endcall %}
    {% endif %}

    {{ return("select 1 as no_op") }}
{% endmacro %}
