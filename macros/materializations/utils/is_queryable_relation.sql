{% macro is_queryable_relation(relation) %}

    {% set query -%}
        select 1 from {{ relation }} where false limit 0
    {%- endset %}

    {{ return(is_queryable(query)) }}

{% endmacro %}
