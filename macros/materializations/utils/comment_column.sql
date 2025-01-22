{%- macro comment_column(relation, column, description) -%}

comment if exists on column {{ relation }}.{{ column }} is $${{ description }}$$

{%- endmacro -%}
