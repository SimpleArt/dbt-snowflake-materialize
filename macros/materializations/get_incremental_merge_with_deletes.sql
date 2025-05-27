{%- macro get_incremental_merge_with_deletes_sql(arg_dict) -%}

{%- set unique_key = arg_dict["unique_key"] -%}
{%- set columns = arg_dict["dest_columns"] -%}

merge into
    {{ arg_dict["target_relation"] }} as destination
using
    {{ arg_dict["temp_relation"] }} as source
on
    {%- if unique_key is string %}
    destination.{{ unique_key }} is not distinct from source.{{ unique_key }}
    {%- else %}
    {% for key in unique_key -%}
    destination.{{ key }} is not distinct from source.{{ key }}
    {%- if not loop.last %}
    and {% endif %}
    {%- endfor %}
    {%- endif %}
    {%- if arg_dict.get("incremental_predicates") is not none %}
    and {{ arg_dict.get("incremental_predicates") }}
    {%- endif %}
when not matched and source.__merge_action__ = 'insert' then
    insert (
        {%- for column in columns %}
        {{ column.name }} {{- "," if not loop.last }}
        {%- endfor %}
    ) values (
        {%- for column in columns %}
        source.{{ column.name }} {{- "," if not loop.last }}
        {%- endfor %}
    )
when matched and source.__merge_action__ = 'update' then
    update set
        {%- for column in columns %}
        {{ column.name }} = source.{{ column.name }} {{- "," if not loop.last }}
        {%- endfor %}
when matched and source.__merge_action__ = 'delete' then
    delete
{%- endmacro -%}
