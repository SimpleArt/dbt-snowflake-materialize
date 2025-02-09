{% macro get_columns_in_query(sql) %}
    {{ return(adapter.dispatch('get_columns_in_query')(sql)) }}
{% endmacro %}

{% macro snowflake__get_columns_in_query(sql) %}

{% set query %}
with get_schema as procedure()
    returns table(columns varchar)
    language python
    runtime_version = 3.11
    packages = ('snowflake-snowpark-python')
    handler = 'get_schema'
as $$
def get_schema(session):
    columns = session.sql(f"""{{ f_string_sql(sql) }}""").columns
    quoted = ", ".join(columns)
    return session.sql(f"select {chr(36)}{chr(36)}{quoted}{chr(36)}{chr(36)} as columns")
$$

call get_schema()
{% endset %}

    {% if execute %}
        {% for row in run_query(query) %}
            {{ return(row['COLUMNS'].split(', ')) }}
        {% endfor %}
    {% endif %}

    {{ return([]) }}

{% endmacro %}
