{% materialization materialized_stream, adapter='snowflake', supported_languages=['sql'] %}
    {% set original_query_tag = set_query_tag() %}
    {% set sql_header = config.get('sql_header') %}

    {% set transient = config.get('transient', false) %}
    {% set change_tracking = config.get('change_tracking', true) %}
    {% set copy_grants = config.get('copy_grants', false) %}
    {% set cluster_by = config.get('cluster_by') %}
    {% set on_schema_change = config.get('on_schema_change', 'evolve_schema') %}

    {% set source_stream_relation = string_to_relation(parse_jinja(config.get('source_stream', ''))['code']) %}

    {% if source_stream_relation is none %}
        {% call statement('invalid_source_stream') %}
            invalid config.source_stream: {{ config.get('source_stream') }}
        {% endcall %}
    {% endif %}

    {% set source_table_relation = string_to_relation(parse_jinja(config.get('source_table', ''))['code']) %}
    {% set refresh_by = config.get('refresh_by') %}

    {% if refresh_by is string %}
        {% set refresh_by = [refresh_by] %}
    {% endif %}

    {% set refresh_deletes = config.get('refresh_deletes', true) %}
    {% set delete_by = config.get('delete_by', refresh_by) %}

    {% if delete_by is string %}
        {% set delete_by = [delete_by] %}
    {% endif %}

    {% set batch_size = config.get('aggregate_batch_size') %}

    {% if batch_size is none %}
        {% set batch_size = 500000 %}
    {% endif %}

    {% if config.get('aggregate') is not none %}
        {% set materialize_mode = 'aggregate' %}
    {% elif refresh_by is none %}
        {% set materialize_mode = 'table' %}
    {% else %}
        {% set materialize_mode = 'batch_refresh' %}
        {% set source_relation = config.get('source_relation', '') %}
        {% set source_relation = string_to_relation(parse_jinja(source_relation)['code']) %}

        {% if source_relation is none %}
            {% call statement('invalid_source_relation') %}
                invalid config:
                    source_relation: {{ config.get('source_relation') }}
            {% endcall %}
        {% endif %}
    {% endif %}

    {% set aggregate_columns = [] %}
    {% set aggregate = [] %}
    {% set count_aggs = [] %}

    {% for column, agg in config.get('aggregate', {}).items()
        if agg in ['count_agg', 'max', 'min', 'sum'] %}
            {% set column = get_fully_qualified_identifier(column) %}
            {% do aggregate_columns.append(column) %}
            {% do aggregate.append([column, agg]) %}

            {% if agg == 'count_agg' %}
                {% do count_aggs.append(column) %}
            {% endif %}
    {% endfor %}

    {% do aggregate.sort() %}

    {% if materialize_mode == 'aggregate' %}
        {% set agg_hashes = [] %}

        {% for column, agg in aggregate %}
            {% do agg_hashes.append(local_md5(column) ~ local_md5(agg)) %}
        {% endfor %}

        {% set agg_hash = agg_hashes | join('') %}
    {% endif %}

    {% set target_relation = get_fully_qualified_relation(this).incorporate(type='table') %}
    {% set temp_relation = get_fully_qualified_relation(make_temp_relation(target_relation)) %}
    {% set batch_relation = get_fully_qualified_relation(make_temp_relation(temp_relation)).incorporate(type='table') %}
    {% set store_stream_relation = get_fully_qualified_relation(make_temp_relation(batch_relation)) %}

    {% if execute %}
        {% set state = {} %}

        {% for row in run_query(show_relation(source_stream_relation, 'stream')) %}
            {% set comment = row.get('comment', row.get('description', '')) %}
            {% set sql_hash = local_md5(row.get('created_on') | string) %}

            {% if 'Query Hash: ' in comment %}
                {% set sql_hash = sql_hash ~ comment.split('Query Hash: ')[-1] %}
            {% endif %}

            {% if materialize_mode == 'aggregate' %}
                {% set sql_hash = sql_hash ~ agg_hash %}
            {% endif %}

            {% do state.update({
                'sync': row.get('stale_after') | string,
                'sql_hash': local_md5(sql_hash)
            }) %}
        {% endfor %}

        {% set sync = state.get('sync', '') %}
        {% set sql_hash = state.get('sql_hash', '') %}

        {% set state = {} %}

        {% for row in run_query(show_relation(target_relation, 'table')) %}
            {% do state.update({'row': row}) %}
        {% endfor %}

        {% set row = state.get('row', {}) %}
        {% set comment = row.get('comment', row.get('description', '')) %}

        {% if state == {} %}
            {% set DDL = drop_relation_unless(target_relation, 'table') %}
        {% elif ('Query Hash: ' ~ sql_hash) not in comment %}
            {% set DDL = 'create or replace' %}
        {% elif transient and row.get('kind') != 'TRANSIENT' %}
            {% set DDL = 'create or replace' %}
        {% elif not transient and row.get('kind') == 'TRANSIENT' %}
            {% set DDL = 'create or replace' %}
        {% else %}
            {% set DDL = 'create if not exists' %}
        {% endif %}

        {% set state = {'prefix': comment} %}

        {% for metadata in ['Sync: ', 'Query Hash: '] if metadata in state['prefix'] %}
            {% set prefix = state['prefix'] %}
            {% do state.update({'prefix': prefix[:prefix.index(metadata)]}) %}
        {% endfor %}

        {% set prefix = state['prefix'] %}
    {% else %}
        {% set sync = '' %}
        {% set sql_hash = '' %}
        {% set DDL = drop_relation_unless(target_relation, 'table', none, transient) %}
        {% set prefix = '' %}
    {% endif %}

    {% if should_full_refresh() %}
        {% set DDL = 'create or replace' %}
    {% endif %}

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- BEGIN happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% set check_schema = false %}

    {% if execute and DDL == 'create if not exists' and materialize_mode == 'table' %}
        {% set check_schema = true %}

        {% call statement('create_temp_relation') %}
            {{ sql_header if sql_header is not none }}

            create or replace temporary view {{ temp_relation }} as
                select * exclude (metadata$action, metadata$isupdate) from ({{ sql }})
        {% endcall %}

    {% elif execute and materialize_mode == 'batch_refresh' %}
        {% set check_schema = true %}

        {% call statement('save_stream') %}
            create or replace temporary table {{ store_stream_relation }} as
                select * from {{ source_stream_relation }}
        {% endcall %}

        {% call statement('create_refresh_relation') %}
            create or replace temporary table {{ temp_relation }} as
                select
                    source_table.*
                from
                    {{ source_relation }} as source_table
                inner join
                    (
                        select distinct
                            {%- for column in refresh_by %}
                            {{ column }} {{- ',' if not loop.last }}
                            {%- endfor %}
                        from
                            {{ store_stream_relation }}
                        where
                            metadata$action = 'INSERT'
                    ) as source_stream
                        on 1 = 1
                        {%- for column in refresh_by %}
                        and source_stream.{{ column }} is not distinct from source_table.{{ column }}
                        {%- endfor %}
        {% endcall %}

        {% call statement('save_sql') %}
            {{ sql_header if sql_header is not none }}

            create or replace temporary table {{ temp_relation }} as
                select
                    hash(*) as metadata$checksum,
                    *
                from
                    ({{ sql }})
        {% endcall %}

    {% elif execute and materialize_mode == 'aggregate' %}
        {% set check_schema = true %}

        {% if count_aggs | length == 0 %}
            {% call statement('create_temp_relation') %}
                {{ sql_header if sql_header is not none }}

                create or replace temporary table {{ temp_relation }} as
                    select
                        *
                    from
                        (
                            select
                                * exclude (
                                    metadata$action,
                                    metadata$isupdate,
                                    metadata$row_id
                                ) replace (
                                    {%- for column, agg in aggregate %}
                                    {%- if agg == 'sum' %}
                                    zeroifnull(sum(decode(metadata$action, 'INSERT', {{ column }}, -{{ column }}))) as {{ column }}
                                    {%- else %}
                                    {{ agg }}(decode(metadata$action, 'INSERT', {{ column }})) as {{ column }}
                                    {%- endif %} {{- ',' if not loop.last }}
                                    {%- endfor %}
                                ),
                                sum(decode(metadata$action, 'INSERT', 1, -1)) as metadata$row_count
                            from
                                ({{ sql }})
                            group by
                                all
                        )
                    where
                        metadata$row_count != 0
                        {%- for column, agg in aggregate %}
                        {%- if agg == 'sum' %}
                        or {{ column }} != 0
                        {%- else %}
                        or {{ column }} is not null
                        {%- endif %}
                        {%- endfor %}
            {% endcall %}

            {% set columns = adapter.get_columns_in_relation(temp_relation) %}
            {% set dimensions = [] %}

            {% for column in columns
                if column.name not in aggregate_columns
                and adapter.quote(column.name) not in aggregate_columns
                and column.name != 'METADATA$ROW_COUNT' %}
                    {% do dimensions.append(adapter.quote(column.name)) %}
            {% endfor %}

        {% elif count_aggs | length == 1 %}
            {% call statement('create_temp_relation') %}
                {{ sql_header if sql_header is not none }}

                create or replace temporary table {{ temp_relation }} as
                    select
                        * replace (
                            {%- for column, agg in aggregate %}
                            {%- if agg == 'count_agg' %}
                            object_agg({{ column }}, metadata$row_count) as {{ column }},
                            {%- else %}
                            {{ agg }}({{ column }}) as {{ column }},
                            {%- endif %}
                            {%- endfor %}
                            sum(metadata$row_count) as metadata$row_count
                        )
                    from
                        (
                            select
                                * exclude (
                                    metadata$action,
                                    metadata$isupdate,
                                    metadata$row_id
                                ) replace (
                                    {%- for column, agg in aggregate if agg != 'count_agg' %}
                                    {%- if agg == 'sum' %}
                                    zeroifnull(sum(decode(metadata$action, 'INSERT', {{ column }}, -{{ column }}))) as {{ column }}
                                    {%- else %}
                                    {{ agg }}(decode(metadata$action, 'INSERT', {{ column }})) as {{ column }}
                                    {%- endif %} {{- ',' if not loop.last }}
                                    {%- endfor %}
                                ),
                                sum(decode(metadata$action, 'INSERT', 1, -1)) as metadata$row_count
                            from
                                ({{ sql }})
                            group by
                                all
                        )
                    where
                        metadata$row_count != 0
                        {%- for column, agg in aggregate if agg != 'count_agg' %}
                        {%- if agg == 'sum' %}
                        or {{ column }} != 0
                        {%- else %}
                        or {{ column }} is not null
                        {%- endif %}
                        {%- endfor %}
                    group by
                        all
            {% endcall %}

            {% set columns = adapter.get_columns_in_relation(temp_relation) %}
            {% set dimensions = [] %}

            {% for column in columns
                if column.name not in aggregate_columns
                and adapter.quote(column.name) not in aggregate_columns
                and column.name != 'METADATA$ROW_COUNT' %}
                    {% do dimensions.append(adapter.quote(column.name)) %}
            {% endfor %}

        {% else %}
            {% call statement('create_batch_relation') %}
                {{ sql_header if sql_header is not none }}

                create or replace temporary table {{ batch_relation }} as
                    select
                        *
                    from
                        (
                            select
                                * exclude (
                                    metadata$action,
                                    metadata$isupdate,
                                    metadata$row_id
                                ) replace (
                                    {%- for column, agg in aggregate if agg != 'count_agg' %}
                                    {%- if agg == 'sum' %}
                                    zerofinull(sum(decode(metadata$action, 'INSERT', {{ column }}, -{{ column }}))) as {{ column }}
                                    {%- else %}
                                    {{ agg }}(decode(metadata$action, 'INSERT', {{ column }})) as {{ column }}
                                    {%- endif %} {{- ',' if not loop.last }}
                                    {%- endfor %}
                                ),
                                sum(decode(metadata$action, 'INSERT', 1, -1)) as metadata$row_count
                            from
                                ({{ sql }})
                            group by
                                all
                        )
                    where
                        metadata$row_count != 0
                        {%- for column, agg in aggregate if agg != 'count_agg' %}
                        {%- if agg == 'sum' %}
                        or {{ column }} != 0
                        {%- else %}
                        or {{ column }} is not null
                        {%- endif %}
                        {%- endfor %}
            {% endcall %}

            {% call statement('get_count_distinct') %}
                select
                    {%- for column in count_aggs %}
                    approx_count_distinct({{ column }}) as {{ column }},
                    {%- endfor %}
                from
                    {{ batch_relation }}
            {% endcall %}

            {% set state = {} %}

            {% for column, count in zip(count_aggs, load_result('get_count_distinct')['data'][0]) %}
                {% if count is not none and 'count' in state and count > state['count'] %}
                    {% do state.update({'column': column, 'count': count}) %}
                {% elif 'count' not in state and loop.last %}
                    {% do state.update({'column': column, 'count': count}) %}
                {% endif %}
            {% endfor %}

            {% set count_agg_column = state['column'] %}
            {% set columns = adapter.get_columns_in_relation(batch_relation) %}
            {% set dimensions = [] %}

            {% for column in columns
                if column.name not in aggregate_columns
                and adapter.quote(column.name) not in aggregate_columns
                and column.name != 'METADATA$ROW_COUNT' %}
                    {% do dimensions.append(adapter.quote(column.name)) %}
            {% endfor %}

            {% call statement('aggregate_batch_relation') %}
                create or replace temporary table {{ batch_relation }} as
                    select
                        *,
                        row_number() over (
                            order by
                                {%- for dimension in dimensions %}
                                {{ dimension }},
                                {%- endfor %}
                                {%- for column, agg in aggregate
                                    if agg == 'count_agg'
                                    and column != count_agg_column %}
                                {{ column }} {{- ',' if not loop.last }}
                                {%- endfor %}
                        ) as {{ adapter.quote("metadata$row_index") }}
                    from
                        (
                            select
                                * replace (
                                    {%- for column, agg in aggregate %}
                                    {%- if column == count_agg_column %}
                                    object_agg({{ column }}, metadata$row_count) as {{ column }},
                                    {%- elif agg != 'count_agg' %}
                                    {{ agg }}({{ column }}) as {{ column }},
                                    {%- endif %}
                                    {%- endfor %}
                                    sum(metadata$row_count) as metadata$row_count
                                )
                            from
                                {{ batch_relation }}
                            group by
                                all
                        )
                    order by
                        {{ adapter.quote("metadata$row_index") }}
            {% endcall %}

            {% set get_row_count %}
                select count(*) as row_count from {{ batch_relation }}
            {% endset %}

            {% set row_count = run_query(get_row_count)[0]['ROW_COUNT'] %}

            {% if row_count == 0 %}
                {% call statement('create_temp_relation') %}
                    create or replace temporary table {{ temp_relation }} as
                        select
                            * exclude (
                                {{ adapter.quote('metadata$row_index') }}
                            ) replace (
                                {%- for column, agg in aggregate %}
                                {%- if column == count_agg_column %}
                                {{ materialized_count_union_agg() }}({{ column }}) as {{ column }},
                                {%- elif agg == 'count_agg' %}
                                {{ materialized_count_agg() }}({{ column }}, metadata$row_count) as {{ column }},
                                {%- else %}
                                {{ agg }}({{ column }}) as {{ column }},
                                {%- endif %}
                                {%- endfor %}
                                sum(metadata$row_count) as metadata$row_count
                            )
                        from
                            {{ batch_relation }}
                        group by
                            all
                {% endcall %}
            {% endif %}

            {% for i in range(0, row_count, batch_size) %}
                {% if loop.first %}
                    {% call statement('create_temp_relation') %}
                        create or replace temporary table {{ temp_relation }} as
                            select
                                * exclude (
                                    {{ adapter.quote('metadata$row_index') }}
                                ) replace (
                                    {%- for column, agg in aggregate %}
                                    {%- if column == count_agg_column %}
                                    {{ materialized_count_union_agg() }}({{ column }}) as {{ column }},
                                    {%- elif agg == 'count_agg' %}
                                    {{ materialized_count_agg() }}({{ column }}, metadata$row_count) as {{ column }},
                                    {%- else %}
                                    {{ agg }}({{ column }}) as {{ column }},
                                    {%- endif %}
                                    {%- endfor %}
                                    sum(metadata$row_count) as metadata$row_count
                                )
                            from
                                {{ batch_relation }}
                            where
                                {{ i }} < {{ adapter.quote('metadata$row_index') }} <= {{ i + batch_size }}
                            group by
                                all
                    {% endcall %}

                {% else %}
                    {% call statement('batch_merge') %}
                        merge into
                            {{ temp_relation }} as destination
                        using
                            (
                                select
                                    * exclude (
                                        {{ adapter.quote('metadata$row_index') }}
                                    ) replace (
                                        {%- for column, agg in aggregate %}
                                        {%- if column == count_agg_column %}
                                        {{ materialized_count_union_agg() }}({{ column }}) as {{ column }},
                                        {%- elif agg == 'count_agg' %}
                                        {{ materialized_count_agg() }}({{ column }}, metadata$row_count) as {{ column }},
                                        {%- else %}
                                        {{ agg }}({{ column }}) as {{ column }},
                                        {%- endif %}
                                        {%- endfor %}
                                        sum(metadata$row_count) as metadata$row_count
                                    )
                                from
                                    {{ batch_relation }}
                                where
                                    {{ i }} < {{ adapter.quote('metadata$row_index') }} <= {{ i + batch_size }}
                                group by
                                    all
                            ) as source
                        on
                            1 = 1
                            {%- for dimension in dimensions %}
                            and destination.{{ dimension }} is not distinct from source.{{ dimension }}
                            {%- endfor %}
                        when matched and destination.metadata$row_count + source.metadata$row_count = 0 then
                            delete
                        when matched and destination.metadata$row_count + source.metadata$row_count != 0 then
                            update set
                                {%- for column, agg in aggregate %}
                                {%- if agg == 'count_agg' %}
                                {{ column }} = {{ materialized_count_add() }}(destination.{{ column }}, source.{{ column }}),
                                {%- elif agg == 'sum' %}
                                {{ column }} = destination.{{ column }} + source.{{ column }},
                                {%- elif agg == 'max' %}
                                {{ column }} = greatest_ignore_nulls(destination.{{ column }}, source.{{ column }}),
                                {%- elif agg == 'min' %}
                                {{ column }} = least_ignore_nulls(destination.{{ column }}, source.{{ column }}),
                                {%- endif %}
                                {%- endfor %}
                                metadata$row_count = destination.metadata$row_count + source.metadata$row_count
                        when not matched then
                            insert (
                                {%- for column in columns %}
                                {{ adapter.quote(column.name) }} {{- ',' if not loop.last }}
                                {%- endfor %}
                            ) values (
                                {%- for column in columns %}
                                source.{{ adapter.quote(column.name) }} {{- ',' if not loop.last }}
                                {%- endfor %}
                            )
                    {% endcall %}

                {% endif %}
            {% endfor %}

            {% call statement('drop_batch_relation') %}
                drop table if exists {{ batch_relation }}
            {% endcall %}

        {% endif %}
    {% endif %}

    {% if execute and check_schema and DDL == 'create if not exists' %}
        {% set temp_type = 'table' %}

        {% if materialize_mode == 'table' %}
            {% set temp_type = 'view' %}
        {% endif %}

        {% if evolve_schema(
            temp_relation.incorporate(type=temp_type),
            target_relation,
            transient,
            (on_schema_change == 'evolve_schema')
        ) %}
            {% if on_schema_change == 'fail' %}
                {% call statement('fail_schema_change') %}
                    config.on_schema_change: fail
                        fix: use the dbt --full-refresh flag
                {% endcall %}
            {% elif on_schema_change != 'evolve_schema' %}
                {% set DDL = 'create or replace' %}
            {% endif %}
        {% endif %}

        {% if materialize_mode == 'table' %}
            {% call statement('drop_temp_relation') %}
                drop view if exists {{ temp_relation }}
            {% endcall %}
        {% endif %}
    {% endif %}

    {% if execute and materialize_mode == 'batch_refresh' and DDL == 'create if not exists' %}
        {% call statement('create_delete_relation') %}
            create or replace temporary table {{ batch_relation }} as
                select
                    source_table.*
                from
                    {{ target_relation }} as source_table
                inner join
                    (
                        select distinct
                            {%- for column in refresh_by %}
                            {{ column }} {{- ',' if not loop.last }}
                            {%- endfor %}
                        from
                            {{ store_stream_relation }}
                        where
                            metadata$action = 'DELETE'
                    ) as source_stream
                        on 1 = 1
                        {%- for column in refresh_by %}
                        and source_stream.{{ column }} is not distinct from source_table.{{ column }}
                        {%- endfor %}
        {% endcall %}

        {% call statement('save_deltas') %}
            create or replace temporary table {{ store_stream_relation }} as
                with
                    inserts as (
                        select
                            metadata$checksum,
                            {%- if not refresh_deletes %}
                            {%- for column in delete_by %}
                            any_value({{ column }}) as {{ column }},
                            {%- endfor %}
                            {%- endif %}
                            count(*) as {{ adapter.quote('persistent$inserts') }}
                        from
                            {{ temp_relation }}
                        group by
                            1
                    ),

                    deletes as (
                        select
                            metadata$checksum,
                            {%- if not refresh_deletes %}
                            {%- for column in delete_by %}
                            any_value({{ column }}) as {{ column }},
                            {%- endfor %}
                            {%- endif %}
                            count(*) as {{ adapter.quote('persistent$deletes') }}
                        from
                            {{ batch_relation }}
                        group by
                            1
                    ),

                    delta as (
                        select
                            metadata$checksum,
                            inserts.{{ adapter.quote('persistent$inserts') }},
                            deletes.{{ adapter.quote('persistent$deletes') }}
                        from
                            inserts
                        full outer join
                            deletes
                                using(metadata$checksum)
                        {%- if refresh_deletes %}
                        where
                            inserts.{{ adapter.quote('persistent$inserts') }} is distinct from deletes.{{ adapter.quote('persistent$deletes') }}
                        {%- else %}
                        qualify
                            inserts.{{ adapter.quote('persistent$inserts') }} is distinct from deletes.{{ adapter.quote('persistent$deletes') }}
                            and max(inserts.{{ adapter.quote('persistent$inserts') }}) over (
                                partition by
                                    {%- for column in delete_by %}
                                    ifnull(inserts.{{ column }}, deletes.{{ column }}) {{- ',' if not loop.last }}
                                    {%- endfor %}
                            ) is not null
                        {%- endif %}
                    )

                select * from final
        {% endcall %}

        {% set stats_query %}
            select
                zeroifnull(max({{ adapter.quote('persistent$inserts') }})) as insert_dupes,
                zeroifnull(max({{ adapter.quote('persistent$deletes') }})) as delete_dupes,
                zeroifnull(sum({{ adapter.quote('persistent$inserts') }})) as inserts,
                zeroifnull(sum({{ adapter.quote('persistent$deletes') }})) as deletes
            from
                {{ store_stream_relation }}
        {% endset %}

        {% set row = run_query(stats_query)[0] %}

        {% if row['DELETES'] == 0 %}
            {% set persist_strategy = 'insert_only' %}
        {% elif row['INSERTS'] == 0 %}
            {% set persist_strategy = 'delete_only' %}
        {% elif row['INSERT_DUPES'] > 1 or row['DELETE_DUPES'] > 1 %}
            {% set persist_strategy = 'delete+insert' %}
        {% else %}
            {% set persist_strategy = 'merge' %}
        {% endif %}

    {% endif %}

    {% if DDL == 'create if not exists' %}
        {% if not change_tracking %}
            {% call statement('unset_change_tracking') %}
                alter table if exists {{ target_relation }} set
                    change_tracking = false
            {% endcall %}
        {% endif %}

        {% if cluster_by is none %}
            {% call statement('drop_cluster_by') %}
                alter table if exists {{ target_relation }}
                    drop clustering key
            {% endcall %}
        {% endif %}
    {% endif %}

    --------------------------------------------------------------------------------------------------------------------
    -- build model

    {% if DDL == 'create or replace' %}
        {% call statement('main') %}
            {{ sql_header if sql_header is not none and materialize_mode == 'table' -}}

            create or replace {{- " transient" if transient }} table {{ target_relation }}
                {%- if cluster_by is not none %}
                cluster by ({{ cluster_by | join(', ') }})
                {%- endif %}
                {%- if copy_grants %}
                copy grants
                {%- endif %}
            as
                {%- if materialize_mode == 'table' %}
                select
                    * exclude (metadata$action, metadata$isupdate)
                from
                    ({{ sql }})
                where
                    metadata$action = 'INSERT'
                {%- else %}
                select * from {{ temp_relation }}
                {%- endif %}
                {%- if cluster_by is not none %}
                order by {{ cluster_by | join(', ') }}
                {%- endif %}
        {% endcall %}

    {% elif materialize_mode == 'aggregate' %}
        {% call statement('main') %}
            merge into
                {{ target_relation }} as destination
            using
                {%- if cluster_by is none %}
                {{ temp_relation }} as source
                {%- else %}
                (
                    select *
                    from {{ temp_relation }}
                    order by {{ cluster_by | join(', ') }}
                ) as source
                {%- endif %}
            on
                1 = 1
                {%- for dimension in dimensions %}
                and destination.{{ dimension }} is not distinct from source.{{ dimension }}
                {%- endfor %}
            when matched and destination.metadata$row_count + source.metadata$row_count = 0 then
                delete
            when matched and destination.metadata$row_count + source.metadata$row_count != 0 then
                update set
                    {%- for column, agg in aggregate %}
                    {%- if agg == 'count_agg' %}
                    {{ column }} = {{ materialized_count_add() }}(destination.{{ column }}, source.{{ column }}),
                    {%- elif agg == 'sum' %}
                    {{ column }} = destination.{{ column }} + source.{{ column }},
                    {%- elif agg == 'max' %}
                    {{ column }} = greatest_ignore_nulls(destination.{{ column }}, source.{{ column }}),
                    {%- elif agg == 'min' %}
                    {{ column }} = least_ignore_nulls(destination.{{ column }}, source.{{ column }}),
                    {%- endif %}
                    {%- endfor %}
                    metadata$row_count = destination.metadata$row_count + source.metadata$row_count
            when not matched then
                insert (
                    {%- for column in columns %}
                    {{ adapter.quote(column.name) }} {{- ',' if not loop.last }}
                    {%- endfor %}
                ) values (
                    {%- for column in columns %}
                    source.{{ adapter.quote(column.name) }} {{- ',' if not loop.last }}
                    {%- endfor %}
                )
        {% endcall %}

    {% elif materialize_mode == 'table' %}
        {% set columns = adapter.get_columns_in_relation(target_relation) %}

        {% call statement('main') %}
            {{ sql_header if sql_header is not none }}

            merge into
                {{ target_relation }} as destination
            using
                (
                    select *
                    from ({{ sql }})
                    where metadata$action = 'INSERT' or not metadata$isupdate
                    {%- if cluster_by is not none %}
                    order by {{ cluster_by | join(', ') }}
                    {%- endif %}
                ) as source
            on
                destination.metadata$row_id = source.metadata$row_id
            when not matched and source.metadata$action = 'INSERT' then
                insert (
                    {%- for column in columns %}
                    {{ adapter.quote(column.name) }} {{- ',' if not loop.last }}
                    {%- endfor %}
                ) values (
                    {%- for column in columns %}
                    source.{{ adapter.quote(column.name) }} {{- ',' if not loop.last }}
                    {%- endfor %}
                )
            when matched and source.metadata$action = 'INSERT' then
                update set
                    {%- for column in columns %}
                    {{ adapter.quote(column.name) }} = source.{{ adapter.quote(column.name) }} {{- ',' if not loop.last }}
                    {%- endfor %}
            when matched and source.metadata$action = 'DELETE' then
                delete
        {% endcall %}

    {% elif persist_strategy == 'delete_only' %}
        {% call statement('main') %}
            delete from
                {{ target_relation }} as destination
            using
                {{ store_stream_relation }} as delta
            where
                delta.{{ adapter.quote('persistent$deletes') }} is not null
                and destination.metadata$checksum = delta.metadata$checksum
        {% endcall %}

    {% elif persist_strategy == 'insert_only' %}
        {% call statement('main') %}
            insert into {{ target_relation }}
                select
                    *
                from
                    (
                        select
                            source.*
                        from
                            {{ temp_relation }} as source
                        inner join
                            {{ store_stream_relation }} as delta
                                using(delta.metadata$checksum)
                        where
                            delta.{{ adapter.quote('persistent$inserts') }} is not null
                    )
                {%- if cluster_by is not none %}
                order by {{ cluster_by | join(', ') }}
                {%- endif %}
        {% endcall %}

    {% elif persist_strategy == 'delete+insert' %}
        {% call statement('delete_removed') %}
            delete from
                {{ target_relation }} as destination
            using
                {{ store_stream_relation }} as delta
            where
                destination.metadata$checksum = delta.metadata$checksum
                and delta.{{ adapter.quote('persistent$deletes') }} is not null
        {% endcall %}

        {% call statement('main') %}
            insert into {{ target_relation }}
                select
                    *
                from
                    (
                        select
                            source.*
                        from
                            {{ temp_relation }} as source
                        inner join
                            {{ store_stream_relation }} as delta
                                using(delta.metadata$checksum)
                        where
                            delta.inserts is not null
                    )
                {%- if cluster_by is not none %}
                order by {{ cluster_by | join(', ') }}
                {%- endif %}
        {% endcall %}

    {% else %}
        {% set columns = get_columns_in_relation(target_relation) %}

        {% call statement('main') %}
            merge into
                {{ target_relation }} as destination
            using
                (
                    with
                        delta as (select * from {{ store_stream_relation }}),

                        inserts as (
                            select
                                source.*,
                                'insert' as {{ adapter.quote('persistent$merge_action') }}
                            from
                                {{ temp_relation }} as source
                            inner join
                                delta
                                    using(delta.metadata$checksum)
                            where
                                delta.{{ adapter.quote('persistent$inserts') }} is not null
                        ),

                        deletes as (
                            select
                                destination.*,
                                'delete' as {{ adapter.quote('persistent$merge_action') }}
                            from
                                {{ temp_relation }} as destination
                            inner join
                                delta
                                    using(delta.metadata$checksum)
                            where
                                delta.{{ adapter.quote('persistent$inserts') }} is null
                        )

                    select * from inserts
                    union all
                    select * from deletes
                    {%- if cluster_by is not none %}
                    order by {{ adapter.quote('persistent$merge_action') }}, {{ cluster_by | join(', ') }}
                    {%- endif %}
                ) as source
            on
                destination.metadata$checksum = source.metadata$checksum
            when not matched and source.{{ adapter.quote('persistent$merge_action') }} = 'insert' then
                insert (
                    {%- for column in columns %}
                    {{ adapter.quote(column.name) }} {{- ',' if not loop.last }}
                    {%- endfor %}
                ) values (
                    {%- for column in columns %}
                    source.{{ adapter.quote(column.name) }} {{- ',' if not loop.last }}
                    {%- endfor %}
                )
            when matched and source.{{ adapter.quote('persistent$merge_action') }} = 'insert' then
                update set
                    {%- for column in columns %}
                    {{ adapter.quote(column.name) }} = source.{{ adapter.quote(column.name) }} {{- ',' if not loop.last }}
                    {%- endfor %}
            when matched and source.{{ adapter.quote('persistent$merge_action') }} = 'delete' then
                delete
        {% endcall %}

    {% endif %}

    {% if DDL == 'create if not exists' and cluster_by is not none %}
        {% call statement('set_clustering_key') %}
            alter table if exists {{ target_relation }}
                    cluster by ({{ cluster_by | join(', ') }})
        {% endcall %}
    {% endif %}

    {% if not (config.persist_relation_docs() and model.description) %}
        {% call statement('save_metadata') %}
            alter table {{ target_relation }} set
                comment = $${{ prefix }}Sync: {{ sync }}\nQuery Hash: {{ sql_hash }}$$
        {% endcall %}
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
        {% do custom_persist_docs(target_relation, model, 'table', 'Sync: ' ~ sync ~ '\nQuery Hash: ' ~ sql_hash) %}
    {% endif %}

    {% do unset_query_tag(original_query_tag) %}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
