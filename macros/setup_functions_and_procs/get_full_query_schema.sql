{%- macro get_full_query_schema(query, temp_relation) -%}
    job = session.sql(f"{{ query }}").collect_nowait()
    job.result()
    session.sql(
        f"create or replace temporary table {{ temp_relation }} as\n"
        f"    select * from table(result_scan('{job.query_id}'))"
    ).collect()
    returns = session.sql({{ get_full_table_schema(temp_relation, describe_table(temp_relation).replace('$', '{chr(36)}')) }}).collect()[0][0]
    session.sql("drop table if exists {{ temp_relation }}").collect()
{%- endmacro -%}
