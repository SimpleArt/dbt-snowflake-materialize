{%- macro sql_try_except(query) -%}
with try_query as procedure()
    returns table(status varchar)
    language python
    runtime_version = 3.11
    packages = ('snowflake-snowpark-python')
    handler = 'try_query'
as $$
def try_query(session):
    try:
        session.sql("{{ escape_py_string(query) }}").collect()
    except:
        return session.sql("select 'failure' as status")
    else:
        return session.sql("select 'success' as status")
$$

call try_query()
{%- endmacro -%}
