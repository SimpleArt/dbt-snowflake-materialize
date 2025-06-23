{% macro config_set(obj=()) %}
    {{ return({'set': obj}) }}
{% endmacro %}
