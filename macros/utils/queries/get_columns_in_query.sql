{# get_columns_in_query('select 1 as x') == ['X'] #}

{% macro get_columns_in_query(sql) %}
    {{ return(adapter.dispatch('get_columns_in_query')(sql)) }}
{% endmacro %}

{% macro snowflake__get_columns_in_query(sql) %}

    {% set query | replace('\n        ', '\n') -%}
        with get_columns_in_query as procedure()
            returns table(columns varchar)
            language python
            runtime_version = 3.11
            packages = ('snowflake-snowpark-python')
            handler = 'get_columns_in_query'
        as $$
        import json

        def get_columns_in_query(session):
            df = session.sql("{{ escape_ansii(sql) }}")
            return session.sql("select ? as columns", (json.dumps(df.columns),))
        $$

        call get_columns_in_query()
    {%- endset %}

    {% if execute %}
        {% for row in run_query(query) %}
            {{ return(fromjson(row['COLUMNS'])) }}
        {% endfor %}
    {% endif %}

    {{ return([]) }}

{% endmacro %}
