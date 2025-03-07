{% macro is_queryable(sql) %}
    {% if sql is string %}
        {{ return(adapter.dispatch('is_queryable')(sql)) }}
    {% elif sql.type == 'table' %}
        {{ return(true) }}
    {% else %}
        {{ return(adapter.dispatch('is_queryable')('select 1 as x from ' ~ sql ~ ' where 0=1 ')) }}
    {% endif %}
{% endmacro %}

{% macro snowflake__is_queryable(sql) %}

{% set query -%}
with is_queryable as procedure()
    returns table(result boolean)
    language python
    runtime_version = 3.11
    packages = ('snowflake-snowpark-python')
    handler = 'is_queryable'
as $$
def is_queryable(session):
    try:
        session.sql("""explain {{ escape_py_string(sql) }}""").collect()
    except:
        return session.sql("select false as result")
    else:
        return session.sql("select true as result")
$$

call is_queryable()
{%- endset %}

    {% if execute %}
        {% for row in run_query(query) %}
            {{ return(row['RESULT']) }}
        {% endfor %}
    {% endif %}

    {{ return(false) }}

{% endmacro %}
