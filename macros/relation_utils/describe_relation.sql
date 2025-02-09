{%- macro describe_relation(relation, type=none) -%}
    describe {{ relation.type if type is none else type }} {{ relation }}
{%- endmacro -%}
