{%- macro clean_sql_to_py(sql) -%}
{%- if ";" in sql -%}
with run_sql as procedure()
    returns table()
    language python
    runtime_version = 3.11
    packages = ('snowflake-snowpark-python')
    handler = 'run_sql'
as $$
def run_sql(session):
    return session.sql(f"""{{ sql_f_string(sql) }}""")
$$

call run_sql();
{%- else -%}
{{ sql }}
{%- endif -%}
{%- endmacro -%}
