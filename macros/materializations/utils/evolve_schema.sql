{% macro compare_schema(from_relation, to_relation, transient) %}
select
    case
        when boolor_agg(to_type is not null and from_type is distinct from to_type)
        then
            'create or alter {{- " transient" if transient }} table {{ to_relation }}('
            || listagg(
                case
                    when from_type is not distinct from to_type
                    then '"' || column_name || '" ' || to_type
                end,
                ', '
            ) within group (order by to_position)
            || ')'
    end as drop_columns,
    case
        when boolor_agg(from_type is not null and from_type is distinct from to_type)
        then
            'create or alter {{- " transient" if transient }} table {{ to_relation }}('
            || listagg(
                '"' || column_name || '" ' || from_type,
                ', '
            ) within group (
                order by
                    case
                        when from_type is not distinct from to_type
                        then to_position
                    end,
                    from_position
            )
            || ')'
    end as add_columns
from
    (
        select
            "name" as column_name,
            from_relation."type" as from_type,
            to_relation."type" as to_type
        from
            {%- if from_relation.type == 'view' %}
            table({{ describe_view(from_relation) }}) as from_relation
            {%- else %}
            table({{ describe_table(from_relation) }}) as from_relation
            {%- endif %}
        full outer join
            table({{ describe_table(to_relation) }}) as to_relation
                using("name")
    ) as described
inner join
    (
        select
            *
        from
            (
                select
                    column_name,
                    ordinal_position as from_position
                from
                    {{ to_relation.database }}.information_schema.columns
                where
                    table_name = $${{ to_relation.identifier }}$$
                    and table_schema = $${{ to_relation.schema }}$$
            ) as from_schema
        full outer join
            (
                select
                    column_name,
                    ordinal_position as to_position
                from
                    {{ from_relation.database }}.information_schema.columns
                where
                    table_name = $${{ from_relation.identifier }}$$
                    and table_schema = $${{ from_relation.schema }}$$
            )
                using(column_name)
    )
        using(column_name)
{% endmacro %}

{% macro evolve_schema(from_relation, to_relation, transient) %}
    {% set flag = false %}
    {% if execute %}
        {% set row = run_query(compare_schema(from_relation, to_relation, transient))[0] %}
        {% if row['DROP_COLUMNS'] is not none %}
            {% set flag = true %}
            {% do run_query(row['DROP_COLUMNS']) %}
        {% endif %}
        {% if row['ADD_COLUMNS'] is not none %}
            {% set flag = true %}
            {% do run_query(row['ADD_COLUMNS']) %}
        {% endif %}
    {% endif %}
    {{ return(flag) }}
{% endmacro %}
