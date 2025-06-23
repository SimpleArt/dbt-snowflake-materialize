{# escape_ansii('select \'ABC\' as "XYZ";') == 'select \\x27ABC\\x27 as \\x22XYZ\\x22\\x3b' #}

{% macro escape_ansii(txt) %}
    {{ return(
        (txt | string)
            .replace("\\", "\\x5c")
            .replace("{", "\\x7b")
            .replace("}", "\\x7d")
            .replace(";", "\\x3b")
            .replace("'", "\\x27")
            .replace("$", "\\x24")
            .replace('"', "\\x22")
            .replace("\n", "\\x0a")
            .replace("\r", "\\x0d")
    ) }}
{% endmacro %}
