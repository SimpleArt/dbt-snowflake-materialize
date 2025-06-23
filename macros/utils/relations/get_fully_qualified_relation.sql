{% macro get_fully_qualified_relation(relation) %}
    {{ return(api.Relation.create(
        database=get_fully_qualified_identifier(relation.database),
        schema=get_fully_qualified_identifier(relation.schema),
        identifier=get_fully_qualified_identifier(relation.identifier),
        type=relation.type
    )) }}
{% endmacro %}
