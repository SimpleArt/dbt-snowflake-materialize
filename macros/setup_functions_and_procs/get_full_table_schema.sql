{% macro get_full_table_schema(relation, describe_table_subquery) %}
        f"select listagg('{chr(34)}' || info_schema.column_name || '{chr(34)} ' || describe_table.{chr(34)}type{chr(34)}, ', ')"
        f" within group (order by info_schema.ordinal_position) as full_schema\n"
        f"from table({{ describe_table_subquery }}) as describe_table\n"
        f"inner join {{ relation.database }}.information_schema.columns as info_schema\n"
        f"    on describe_table.{chr(34)}name{chr(34)} = info_schema.column_name\n"
        f"    and info_schema.table_schema = '{{ relation.schema }}'\n"
        f"    and info_schema.table_name = '{{ relation.identifier }}'"
{% endmacro %}
