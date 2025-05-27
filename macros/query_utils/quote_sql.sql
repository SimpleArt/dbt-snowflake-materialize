{% macro quote_sql(sql) %}
    {{ return(sql.replace("\\", "\\\\").replace("'", "\\'")) }}
{% endmacro %}
