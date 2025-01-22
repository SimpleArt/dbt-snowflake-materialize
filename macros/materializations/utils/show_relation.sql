{%- macro show_relation(relation, type=none) -%}

show {{ relation.type if type is none else type }}s like $${{ relation.identifier }}$$ in {{ relation.database }}.{{ relation.schema }}

{%- endmacro -%}
