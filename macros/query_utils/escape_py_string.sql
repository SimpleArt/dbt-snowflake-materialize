{% macro escape_py_string(sql) %}
    {{ return(
        sql
            .replace("\\", "\\x5c")
            .replace("{", "\\x7b")
            .replace("}", "\\x7d")
            .replace(";", "\\x3b")
            .replace("'", "\\x27")
            .replace("$", "\\x24")
            .replace('"', "\\x22")
    ) }}
{% endmacro %}
