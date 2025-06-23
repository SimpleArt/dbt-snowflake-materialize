{% macro is_queryable(query) %}

    {% if query is string %}
        {{ return(adapter.dispatch('is_queryable')(query)) }}
    {% elif query.type == 'table' %}
        {{ return(true) }}
    {% else %}
        {{ return(adapter.dispatch('is_queryable')('select 1 as x from ' ~ query ~ ' where 0=1 limit 0')) }}
    {% endif %}

{% endmacro %}

{% macro snowflake__is_queryable(query) %}

    {% set try_query | replace('\n        ', '\n') %}
        begin
            {{ query }};
            let success_result resultset := (select 'success' as status);
            return table(success_result);
        exception when other then
            let failure_result resultset := (select 'failure' as status);
        end
    {% endset %}

    {% set execute_query | trim %}
        execute immediate '{{ escape_ansii(try_query) }}'
    {% endset %}

    {% if execute %}
        {% for row in run_query(execute_query) %}
            {{ return(row['STATUS'] == 'success') }}
        {% endfor %}
    {% endif %}

    {{ return(false) }}

{% endmacro %}
