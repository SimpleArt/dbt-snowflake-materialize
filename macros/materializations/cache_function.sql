{% materialization cache_function, adapter='snowflake', supported_languages=['sql'] %}
    {% set original_query_tag = set_query_tag() %}
    {% set sql_header = config.get('sql_header') %}

    {% set transient = config.get('transient', false) %}
    {% set change_tracking = config.get('change_tracking', true) %}
    {% set copy_grants = config.get('copy_grants', false) %}
    {% set cluster_by = config.get('cluster_by') %}

    {% set parsed = parse_jinja(config.get('function_as') | string) %}
    {% set function = parsed['code'] %}
    {% set batch_size = config.get('incremental_batch_size') %}

    {% if not (batch_size is number) %}
        {% set batch_size = 500 %}
    {% endif %}

    {% set load_limit = config.get('load_limit') %}

    {% set target_relation = get_fully_qualified_relation(this).incorporate(type='table') %}
    {% set temp_relation = get_fully_qualified_relation(make_temp_relation(target_relation)).incorporate(type='table') %}
    {% set sample_relation = get_fully_qualified_relation(make_temp_relation(temp_relation)).incorporate(type='table') %}

    {% set state = {'sql_hash': local_md5(function | string)} %}

    {% if execute %}
        {% for relation in parsed['relations'] if not (relation is string) %}
            {% for row in run_query(show_relation(relation, 'function')) %}
                {% set comment = row.get('comment', row.get('description', '')) %}
                {% do state.update({'sql_hash': state['sql_hash'] ~ local_md5(comment)}) %}
            {% endfor %}
        {% endfor %}
    {% endif %}

    {% set sql_hash = local_md5(state['sql_hash']) %}

    {% if execute %}
        {% set state = {} %}

        {% for row in run_query(show_relation(target_relation, 'table')) %}
            {% do state.update({'row': row}) %}
        {% endfor %}

        {% set row = state.get('row', {}) %}
        {% set comment = row.get('comment', row.get('description', '')) %}
        {% set prefix = comment %}

        {% if 'Function Hash: ' in prefix %}
            {% set prefix = prefix[:prefix.index('Function Hash: ')] %}
        {% endif %}

        {% if state == {} %}
            {% set DDL = drop_relation_unless(target_relation, 'table') %}
        {% elif ('Function Hash: ' ~ sql_hash) not in comment %}
            {% set DDL = 'create or replace' %}
        {% elif transient and row.get('KIND') != 'TRANSIENT' %}
            {% set DDL = 'create or replace' %}
        {% elif not transient and row.get('KING') == 'TRANSIENT' %}
            {% set DDL = 'create or replace' %}
        {% else %}
            {% set DDL = 'create if not exists' %}
        {% endif %}
    {% else %}
        {% set DDL = drop_relation_unless(target_relation, 'table', ['Function Hash: ' ~ sql_hash], transient) %}
        {% set prefix = '' %}
    {% endif %}

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- BEGIN happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% call statement('create_temp_relation') %}
        create or replace temporary table {{ temp_relation }} as
            select distinct * from ({{ sql }})
    {% endcall %}

    {% call statement('create_sample') %}
        create or replace temporary table {{ sample_relation }} as
            select * from {{ temp_relation }} where 0 = 1
    {% endcall %}

    {% call statement('call_sample') %}
        create or replace temporary table {{ sample_relation }} as
            select *, {{ function }} from {{ sample_relation }}
    {% endcall %}

    {% set compare_schema %}
        select
            count(distinct h) as schema_count
        from
            (
                select
                    hash_agg(
                        * exclude (
                            table_catalog,
                            table_schema,
                            table_name,
                            column_default,
                            is_nullable,
                            schema_evolution_record,
                            comment
                        )
                    ) as h
                from
                    {{ database }}.information_schema.columns
                where
                    table_schema ilike $${{ target_relation.schema }}$$
                    and table_name ilike any ($${{ target_relation.identifier }}$$, $${{ sample_relation.identifier }}$$)
                group by
                    table_name
            )
    {% endset %}

    {% if DDL == 'create if not exists' and execute and run_query(compare_schema)[0]['SCHEMA_COUNT'] == 2 %}
        {% set DDL = 'create or replace' %}
    {% endif %}

    {% set temp_columns = adapter.get_columns_in_relation(temp_relation) %}
    {% set sample_columns = adapter.get_columns_in_relation(sample_relation) %}

    --------------------------------------------------------------------------------------------------------------------
    -- build model
    {% if DDL == 'create or replace' %}

        {% call statement('main') %}
            create or replace {{- ' transient' if transient }} table {{ target_relation }}
                {%- if cluster_by is not none %}
                cluster by ({{ cluster_by | join(', ') }})
                {%- endif %}
                {%- if copy_grants %}
                copy grants
                {%- endif %}
            as
                select * from {{ sample_relation }}
        {% endcall %}

        {% call statement('save_metadata') %}
            alter table {{ target_relation }} set
                comment = $${{ prefix }}Function Hash: {{ sql_hash }}$$
        {% endcall %}

    {% else %}

        {% call statement('main') %}
            insert into {{ target_relation }}
                select * from {{ sample_relation }}
        {% endcall %}

    {% endif %}

    {% call statement('filter_temp_relation') %}
        create or replace temporary table {{ temp_relation }} as
            select
                *,
                row_number() over (order by null) as "metadata$row_index"
            from
                (
                    select * from {{ temp_relation }}

                    minus

                    select
                        {%- for column in temp_columns %}
                        {{ adapter.quote(column.name) }},
                        {%- endfor %}
                    from
                        {{ target_relation }}

                    {%- if load_limit is not none %}

                    limit
                        {{ load_limit }}
                    {%- endif %}
                )
            order by
                "metadata$row_index"
    {% endcall %}

    {% if execute %}
        {% set row_count = run_query('select count(*) as row_count from ' ~ temp_relation)[0]['ROW_COUNT'] %}

        {% for i in range(0, row_count, batch_size) %}
            {% call statement('create_sample') %}
                create or replace temporary table {{ sample_relation }} as
                    select * exclude "metadata$row_index" from {{ temp_relation }} where "metadata$row_index" between {{ i + 1 }} and {{ i + batch_size }}
            {% endcall %}

            {% call statement('call_sample') %}
                merge into
                    {{ target_relation }} as destination
                using
                    (select *, {{ function }} from {{ sample_relation }}) as source
                on
                    1 = 1
                    {%- for column in temp_columns %}
                    and destination.{{ adapter.quote(column.name) }} is not distinct from source.{{ adapter.quote(column.name) }}
                    {%- endfor %}
                when not matched then
                    insert (
                        {%- for column in sample_columns %}
                        {{ adapter.quote(column.name) }} {{- ',' if not loop.last }}
                        {%- endfor %}
                    ) values (
                        {%- for column in sample_columns %}
                        source.{{ adapter.quote(column.name) }} {{- ',' if not loop.last }}
                        {%- endfor %}
                    )
            {% endcall %}
        {% endfor %}
    {% endif %}

    {% if change_tracking %}
        {% call statement('set_change_tracking') %}
            alter table if exists {{ target_relation }} set
                change_tracking = true
        {% endcall %}
    {% endif %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {% if config.get('grants') is not none %}
        {% do custom_apply_grants(target_relation, config.get('grants')) %}
    {% endif %}

    {% if config.persist_relation_docs() %}
        {% do custom_persist_docs(target_relation, model, 'table', '\nFunction Hash: ' ~ sql_hash) %}
    {% endif %}

    {% do unset_query_tag(original_query_tag) %}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
