{% macro sql_run_safe(query) %}

{% if ';' in query %}

{% set query -%}
with run_query as procedure()
    returns table()
    language python
    runtime_version = 3.11
    packages = ('snowflake-snowpark-python')
    handler = 'run_query'
as $$
def run_query(session):
    return session.sql("{{ escape_py_string(query) }}")
$$

call run_query()
{%- endset %}

{% endif %}

{{ return(query) }}

{% endmacro %}
