{%- macro comment_model(relation, description, type=none) -%}

comment if exists on {{ relation.type if type is none else type }} {{ relation }} is $${{ description }}$$

{%- endmacro -%}
