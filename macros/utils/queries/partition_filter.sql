{%- macro partition_filter() -%}
    {%- set s = local_md5(run_started_at | string) %}
    where '{{ s }}' = '{{ s }}'
{%- endmacro -%}
