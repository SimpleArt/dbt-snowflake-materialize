{%- macro get_stream_last_queried_timestamp(relation) -%}
    select convert_timezone('UTC', greatest_ignore_nulls("created_on", "stale_after" - interval '{{ var("stream_retention_days", 14) }} days'))::timestamp_ntz from table({{ show_streams(relation) }})
{%- endmacro -%}
