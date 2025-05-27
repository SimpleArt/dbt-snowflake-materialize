{% macro evolve_schema(from_relation, to_relation, transient) %}

{% set compare_schema %}
describe {{ 'view' if from_relation.type == 'view' else 'table' }} {{ from_relation }}
    ->> describe table {{ to_relation }}
    ->>
        select
            row_number() over (order by schema_relation.ordinal_position) as ordinal_position,
            schema_relation.column_name,
            describe_relation."type" as column_type
        from
            $2 as describe_relation
        inner join
            {{ from_relation.database }}.information_schema.columns as schema_relation
                on describe_relation."name" = schema_relation.column_name
        where
            schema_relation.table_name = $${{ from_relation.identifier }}$$
            and schema_relation.table_schema = $${{ from_relation.schema }}$$
    ->>
        select
            row_number() over (order by schema_relation.ordinal_position) as ordinal_position,
            schema_relation.column_name,
            describe_relation."type" as column_type
        from
            $2 as describe_relation
        inner join
            {{ to_relation.database }}.information_schema.columns as schema_relation
                on describe_relation."name" = schema_relation.column_name
        where
            schema_relation.table_name = $${{ to_relation.identifier }}$$
            and schema_relation.table_schema = $${{ to_relation.schema }}$$
    ->>
        with
            from_schema as (select * from $2),
            to_schema as (select * from $1),

            compare_schema as (
                select
                    column_name,
                    column_type,
                    from_schema.ordinal_position as from_position,
                    to_schema.ordinal_position as to_position
                from
                    from_schema
                full outer join
                    to_schema
                        using(column_name, column_type)
            ),

            keep_schema as (
                select
                    column_name,
                    column_type,
                    from_position as ordinal_position,
                    row_number() over (order by ordinal_position) = ordinal_position as keep_column
                from
                    compare_schema
                where
                    from_position = to_position
            ),

            filtered_schema as (
                select
                    iff(keep_column, column_name, $$0$$) as column_name,
                    column_type,
                    ordinal_position
                from
                    keep_schema
                where
                    keep_column or ordinal_position = 1
            ),

            keep_hashed as (
                select
                    hash_agg(
                        column_name,
                        column_type,
                        ordinal_position
                    ) as hashed
                from
                    filtered_schema
            ),

            from_hashed as (
                select
                    hash_agg(
                        column_name,
                        column_type,
                        ordinal_position
                    ) as hashed
                from
                    from_schema
            ),

            to_hashed as (
                select
                    hash_agg(
                        column_name,
                        column_type,
                        ordinal_position
                    ) as hashed
                from
                    to_schema
            ),

            create_or_alter_keep_schema as (
                select
                    1 as step,
                    (
                        $$create or alter {{- " transient" if transient }} table {{ to_relation }}($$
                        || listagg(
                            $$"$$ || column_name || $$" $$ || column_type,
                            $$, $$
                        )
                        || $$)$$
                    ) as query
                from
                    filtered_schema, keep_hashed, from_hashed
                where
                    keep_hashed.hashed != from_hashed.hashed
                having
                    count(*) > 0
            ),

            create_or_alter_to_schema as (
                select
                    2 as step,
                    (
                        $$create or alter {{- " transient" if transient }} table {{ to_relation }}($$
                        || listagg(
                            $$"$$ || column_name || $$" $$ || column_type,
                            $$, $$
                        )
                        || $$)$$
                    ) as query
                from
                    to_schema, keep_hashed, to_hashed
                where
                    keep_hashed.hashed != to_hashed.hashed
                having
                    count(*) > 0
            ),

            merged as (
                select * from create_or_alter_keep_schema
                union all
                select * from create_or_alter_to_schema
            )

        select query from merged order by step
{% endset %}

    {% set queries = [] %}
    {% if execute %}
        {% for row in run_query(sql_run_safe(compare_schema)) %}
            {% do queries.append(row[0]) %}
        {% endfor %}
    {% endif %}
    {{ return(queries) }}
{% endmacro %}
