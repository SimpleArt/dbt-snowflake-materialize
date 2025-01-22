{% macro is_queryable(query) %}

{% set query -%}
with is_queryable as procedure()
    returns table(result boolean)
    language python
    runtime_version = 3.11
    packages = ('snowflake-snowpark-python')
    handler = 'is_queryable'
as $$
def run_sql(session):
    try:
        session.sql(f"""explain {{ sql_f_string(sql) }}""").collect()
    except:
        return session.sql("select false as result")
    else:
        return session.sql("select true as result")
$$

call is_queryable();
{%- endset %}

    {% if execute %}
        {% for row in run_query(query) %}
            {{ return(row['RESULT']) }}
        {% endfor %}
    {% endif %}

    {{ return(false) }}

{% endmacro %}
