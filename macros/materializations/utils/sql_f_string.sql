{% macro sql_f_string(sql) %}

    {{ return(sql.replace(';', '{chr(59)}').replace('$', '{chr(36)}')) }}

{% endmacro %}
