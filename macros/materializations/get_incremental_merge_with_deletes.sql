{%- macro get_incremental_merge_with_deletes_sql(arg_dict) -%}

{%- set unique_key = arg_dict["unique_key"] -%}
{%- set columns =  -%}
{%- set merge_action = arg_dict["merge_action"] -%}

{%- if unique_key is string -%}
    {%- set unique_key = [unique_key] -%}
{%- endif -%}

merge into
    {{ arg_dict["target_relation"] }} as DBT_INTERNAL_DEST
using
    {{ arg_dict["temp_relation"] }} as DBT_INTERNAL_SOURCE
on
    {%- for key in unique_key %}
    {{ 'and ' if not loop.first -}} DBT_INTERNAL_DEST.{{ key }} is not distinct from DBT_INTERNAL_SOURCE.{{ key }}
    {%- endfor %}
    {%- if arg_dict.get("incremental_predicates") is not none %}
    and {{ arg_dict.get("incremental_predicates") }}
    {%- endif %}
when not matched and DBT_INTERNAL_SOURCE.{{ arg_dict["merge_action"] }} = 'insert' then
    insert (
        {%- for column in arg_dict["dest_columns"] %}
        {{ adapter.quote(column.name) }} {{- "," if not loop.last }}
        {%- endfor %}
    ) values (
        {%- for column in columns %}
        DBT_INTERNAL_SOURCE.{{ column.name }} {{- "," if not loop.last }}
        {%- endfor %}
    )
when matched and DBT_INTERNAL_SOURCE.{{ arg_dict["merge_action"] }} = 'update' then
    update set
        {%- for column in arg_dict["dest_columns"] %}
        {{ adapter.quote(column.name) }} = DBT_INTERNAL_SOURCE.{{ adapter.quote(column.name) }} {{- "," if not loop.last }}
        {%- endfor %}
when matched and DBT_INTERNAL_SOURCE.{{ arg_dict["merge_action"] }} = 'delete' then
    delete
{%- endmacro -%}
