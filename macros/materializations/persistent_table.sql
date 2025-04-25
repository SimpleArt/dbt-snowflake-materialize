{% materialization persistent_table, adapter='snowflake', supported_languages=['sql'] %}
    {% set original_query_tag = set_query_tag() %}
    {% set sql_header = config.get('sql_header') %}

    {% set transient = config.get('transient', true) %}
    {% set copy_grants = config.get('copy_grants', false) %}

    {% set alter_if = [] %}

    {% set row_access_policy = config.get('row_access_policy') %}

    {% if row_access_policy is not none %}
        {% set row_access_policy = parse_jinja(row_access_policy|string)['code'] %}
        {% set row_access_policy_hash = local_md5(row_access_policy|string) %}
        {% do alter_if.append('Row Access Policy Hash: ' ~ row_access_policy_hash) %}
    {% endif %}

    {% set unique_key = config.get('unique_key') %}
    {% set checksum = config.get('check_cols') %}

    {% if unique_key is none %}
        {% set persist_strategy = config.get('persist_strategy', 'insert_overwrite') %}
    {% else %}
        {% set persist_strategy = config.get('persist_strategy', 'merge') %}
    {% endif %}

    {% set on_schema_change = config.get('on_schema_change', 'evolve_schema') %}

    {% set change_tracking = config.get('change_tracking') %}

    {% if change_tracking is not none %}
        {% set change_tracking_hash = local_md5(change_tracking|string) %}
        {% do alter_if.append('Change Tracking Hash: ' ~ change_tracking_hash) %}
    {% endif %}

    {% set tmp_relation_type = config.get('tmp_relation_type', 'view') %}
    {% set cluster_by = config.get('cluster_by') %}

    {% if cluster_by is not none %}
        {% set cluster_by_hash = local_md5(cluster_by|string) %}
        {% do alter_if.append('Cluster By Hash: ' ~ cluster_by_hash) %}
    {% endif %}

    {% if tmp_relation_type not in ['table', 'view'] %}
        {% set tmp_relation_type = 'view' %}
    {% endif %}

    {% set all_keys = [] %}

    {% if unique_key is string %}
        {% set all_keys = [unique_key] %}
    {% elif unique_key is none %}
        {% set all_keys = [] %}
    {% else %}
        {% set all_keys = array_unique(unique_key) %}
    {% endif %}

    {% if checksum is none %}
        {% set all_checksums = [adapter.quote('METADATA$CHECKSUM')] %}
    {% elif checksum is string %}
        {% set all_checksums = [checksum] %}
    {% else %}
        {% set all_checksums = array_unique(checksum) %}
    {% endif %}

    {% set target_relation = get_fully_qualified_relation(this).incorporate(type='table') %}
    {% set temp_relation = get_fully_qualified_relation(make_temp_relation(target_relation).incorporate(type=tmp_relation_type)) %}
    {% set delta_keys_relation = make_temp_relation(temp_relation).incorporate(type='table') %}
    {% set delta_relation = make_temp_relation(delta_keys_relation).incorporate(type='table') %}

    {% if alter_if == [] %}
        {% set alter_if = none %}
    {% endif %}

    {% set drop_result = drop_relation_unless(
        target_relation, 'table', none, transient, alter_if=alter_if
    ) %}

    {% set DDL = drop_result['DDL'] %}

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=false) }}

    -- BEGIN happens here:
    {{ run_hooks(pre_hooks, inside_transaction=true) }}

    --------------------------------------------------------------------------------------------------------------------
    -- build model

    {% if DDL != 'create or replace' %}
        {% call statement('setup') %}
            {{ sql_header if sql_header is not none }}

            create or replace temporary {{ tmp_relation_type }} {{ temp_relation }} as
                {%- if checksum is none and persist_strategy in ['delete+insert', 'merge'] %}
                select hash(*) as {{ all_checksums[0] }}, * from ({{ sql }})
                {%- else %}
                {{ sql }}
                {%- endif %}
        {% endcall %}

        {% set queries = evolve_schema(temp_relation, target_relation, transient) %}

        {% if queries != [] %}
            {% if on_schema_change == 'fail' %}
                {% call statement('fail_schema_change') %}
                    config.on_schema_change: fail
                        fix: use the dbt --full-refresh flag
                {% endcall %}
            {% elif on_schema_change == 'evolve_schema' %}
                {% set persist_strategy = 'insert_overwrite' %}
                {% for query in queries %}
                    {% do run_query(query) %}
                {% endfor %}
            {% else %}
                {% set DDL = 'evolve schema' %}
            {% endif %}
        {% endif %}
    {% endif %}

    {% if DDL in ['create or replace', 'evolve schema'] or not execute
        or persist_strategy not in ['delete+insert', 'insert_overwrite', 'merge']
    %}
        {% set DDL = DDL %}

    {% elif persist_strategy in ['delete+insert', 'merge'] %}
        {% set union_keys = array_union(all_keys, all_checksums) %}
        {% set must_keys = all_keys %}
        {% set is_must_unique = true %}
        {% set is_union_unique = all_keys != [] %}

        {% if all_keys == [] %}
            {% set must_keys = union_keys %}
        {% endif %}

        {% set check_must_unique %}
            select
                count(*) as row_count,
                count(distinct hash({{ must_keys | join(', ') }})) as distinct_count
            from
                {{ target_relation }}
        {% endset %}

        {% set row = run_query(check_must_unique)[0] %}

        {% if row['ROW_COUNT'] != row['DISTINCT_COUNT'] %}
            {% set is_must_unique = false %}
        {% endif %}

        {% set check_union_unique %}
            select
                count(*) as row_count,
                count(distinct hash({{ union_keys | join(', ') }})) as distinct_count
            from
                {{ target_relation }}
        {% endset %}

        {% if not is_must_unique and is_union_unique %}
            {% set row = run_query(check_union_unique)[0] %}
            {% if row['ROW_COUNT'] != row['DISTINCT_COUNT'] %}
                {% set is_union_unique = false %}
            {% endif %}
        {% endif %}

        {% set check_must_unique %}
            select
                count(*) as row_count,
                count(distinct hash({{ must_keys | join(', ') }})) as distinct_count
            from
                {{ temp_relation }}
        {% endset %}

        {% if is_must_unique %}
            {% set row = run_query(check_must_unique)[0] %}
            {% if row['ROW_COUNT'] != row['DISTINCT_COUNT'] %}
                {% set is_must_unique = false %}
            {% endif %}
        {% endif %}

        {% set check_union_unique %}
            select
                count(*) as row_count,
                count(distinct hash({{ union_keys | join(', ') }})) as distinct_count
            from
                {{ temp_relation }}
        {% endset %}

        {% if not is_must_unique and is_union_unique %}
            {% set row = run_query(check_union_unique)[0] %}
            {% if row['ROW_COUNT'] != row['DISTINCT_COUNT'] %}
                {% set is_union_unique = false %}
            {% endif %}
        {% endif %}

        {% if is_must_unique %}
            {% set incremental_keys = must_keys %}
        {% else %}
            {% set incremental_keys = union_keys %}
        {% endif %}

        {% if is_must_unique or is_union_unique %}
            {% call statement('delta_keys') %}
                create or replace temporary table {{ delta_keys_relation }} as
                    with
                        source as (select *, 1 as {{ adapter.quote('persistent$source_row_count') }} from {{ temp_relation }}),
                        destination as (select *, 1 as {{ adapter.quote('persistent$destination_row_count') }} from {{ target_relation }})

                    select
                        {%- for column in incremental_keys %}
                        ifnull(source.{{ column }}, destination.{{ column }}) as {{ column }},
                        {%- endfor %}
                        {{ adapter.quote('persistent$source_row_count') }},
                        {{ adapter.quote('persistent$destination_row_count') }}
                    from
                        source
                    full outer join
                        destination
                            {%- for column in incremental_keys %}
                            {{ "on " if loop.first else "and " -}}
                            source.{{ column }} is not distinct from destination.{{ column }}
                            {%- endfor %}
                    where
                        source.{{ adapter.quote('persistent$source_row_count') }} is distinct from destination.{{ adapter.quote('persistent$destination_row_count') }}
                        {%- for column in all_checksums %}
                        or source.{{ column }} is distinct from destination.{{ column }}
                        {%- endfor %}
            {% endcall %}
        {% else %}
            {% call statement('delta_keys') %}
                create or replace temporary table {{ delta_keys_relation }} as
                    with
                        source as (
                            select
                                {%- for column in union_keys %}
                                {{ column }},
                                {%- endfor %}
                                count(*) as {{ adapter.quote('persistent$row_count') }}
                            from
                                {{ temp_relation }}
                            group by
                                all
                        ),

                        destination as (
                            select
                                {%- for column in union_keys %}
                                {{ column }},
                                {%- endfor %}
                                count(*) as {{ adapter.quote('persistent$row_count') }}
                            from
                                {{ target_relation }}
                            group by
                                all
                        ),

                        delta as (
                            select
                                {%- for column in union_keys %}
                                ifnull(source.{{ column }}, destination.{{ column }}) as {{ column }},
                                {%- endfor %}
                                source.{{ adapter.quote('persistent$row_count') }} as {{ adapter.quote('persistent$source_row_count') }},
                                destination.{{ adapter.quote('persistent$row_count') }} as {{ adapter.quote('persistent$destination_row_count') }}
                            from
                                source
                            full outer join
                                destination
                                    {%- for column in union_keys %}
                                    {{ "on" if loop.first else "and" }} source.{{ column }} is not distinct from destination.{{ column }}
                                    {%- endfor %}
                            where
                                source.{{ adapter.quote('persistent$row_count') }} is distinct from destination.{{ adapter.quote('persistent$row_count') }}
                        ),

                        {%- if all_keys == [] or persist_strategy == 'delete+insert' %}

                        final as (
                            select
                                *,
                                {{ adapter.quote('persistent$source_row_count') }} as {{ adapter.quote('persistent$source_key_count') }},
                                {{ adapter.quote('persistent$destination_row_count') }} as {{ adapter.quote('persistent$destination_key_count') }}
                            from
                                delta
                        )

                        {%- else %}

                        source_counts as (
                            select
                                {%- for column in all_keys %}
                                {{ column }},
                                {%- endfor %}
                                sum({{ adapter.quote('persistent$row_count') }}) as {{ adapter.quote('persistent$row_count') }}
                            from
                                source
                            group by
                                all
                        ),

                        destination_counts as (
                            select
                                {%- for column in all_keys %}
                                {{ column }},
                                {%- endfor %}
                                sum({{ adapter.quote('persistent$row_count') }}) as {{ adapter.quote('persistent$row_count') }}
                            from
                                destination
                            group by
                                all
                        ),

                        final as (
                            select
                                delta.*,
                                source_counts.{{ adapter.quote('persistent$row_count') }} as {{ adapter.quote('persistent$source_key_count') }},
                                destination_counts.{{ adapter.quote('persistent$row_count') }} as {{ adapter.quote('persistent$destination_key_count') }}
                            from
                                delta
                            left join
                                source_counts
                                    {%- for column in all_keys %}
                                    {{ "on" if loop.first else "and" }} delta.{{ column }} is not distinct from source_counts.{{ column }}
                                    {%- endfor %}
                            left join
                                destination_counts
                                    {%- for column in all_keys %}
                                    {{ "on" if loop.first else "and" }} delta.{{ column }} is not distinct from destination_counts.{{ column }}
                                    {%- endfor %}
                        )

                        {%- endif %}

                    select * from final
            {% endcall %}

            {% set stats_query %}
                select
                    zeroifnull(max({{ adapter.quote('persistent$source_row_count') }})) as source_row_count,
                    zeroifnull(max({{ adapter.quote('persistent$source_key_count') }})) as source_key_count,
                    zeroifnull(max({{ adapter.quote('persistent$destination_row_count') }})) as destination_row_count,
                    zeroifnull(max({{ adapter.quote('persistent$destination_key_count') }})) as destination_key_count,
                    zeroifnull(sum({{ adapter.quote('persistent$source_row_count') }})) as inserts,
                    zeroifnull(sum({{ adapter.quote('persistent$destination_row_count') }})) as deletes
                from
                    {{ delta_keys_relation }}
            {% endset %}

            {% if persist_strategy == 'merge' %}
                {% set row = run_query(stats_query)[0] %}

                {% if row['DELETES'] == 0 %}
                    {% set persist_strategy = 'insert_only' %}
                {% elif row['INSERTS'] == 0 %}
                    {% set persist_strategy = 'delete_only' %}
                {% elif row['SOURCE_ROW_COUNT'] > 1 or row['DESTINATION_ROW_COUNT'] > 1 %}
                    {% set persist_strategy = 'delete+insert' %}
                {% elif all_keys != [] and row['SOURCE_KEY_COUNT'] <= 1 and row['DESTINATION_KEY_COUNT'] <= 1 %}
                    {% set incremental_keys = all_keys %}

                    {% call statement('delta_keys_aggregated') %}
                        create or replace temporary table {{ delta_keys_relation }} as
                            select
                                {%- for column in all_keys %}
                                {{ column }},
                                {%- endfor %}
                                sum({{ adapter.quote('persistent$source_row_count') }}) as {{ adapter.quote('persistent$source_row_count') }},
                                sum({{ adapter.quote('persistent$destination_row_count') }}) as {{ adapter.quote('persistent$destination_row_count') }}
                            from
                                {{ delta_keys_relation }}
                            group by
                                all
                    {% endcall %}
                {% endif %}
            {% endif %}
        {% endif %}

    {% endif %}

    {% if DDL == 'alter if exists' and 'alter_if' in drop_result %}
        {% set alter_table %}
            with alter_table as procedure()
                returns table()
            as $$
                begin
                    {%- for part in drop_result['alter_if'] %}
                    {%- if part.startswith('Row Access Policy') %}
                    alter table if exists {{ target_relation }} drop all row access policies;
                    alter table if exists {{ target_relation }} add row access policy {{ row_access_policy }};
                    {%- elif part.startswith('Change Tracking') and not change_tracking %}
                    alter table if exists {{ target_relation }} set change_tracking = false;
                    {%- elif part.startswith('Cluster By') and cluster_by == [] %}
                    alter table if exists {{ target_relation }} drop clustering key;
                    {%- endif %}
                    {%- endfor %}
                    let res resultset := (
                        alter table if exists {{ target_relation }} set comment = '{{ alter_if | join('\\n') }}'
                    );
                    return table(res);
                end
            $$

            call alter_table()
        {% endset %}

        {% call statement('alter_table') %}
            {{ sql_run_safe(alter_table) }}
        {% endcall %}
    {% endif %}

    {% if DDL in ['create or replace', 'evolve schema'] %}
        {% call statement('main') %}
            {{ sql_header if sql_header is not none and DDL == 'create or replace' }}

            create or replace {{- ' transient' if transient }} table {{ target_relation }}
                {%- if cluster_by is not none %}
                cluster by ({{ cluster_by | join(', ') }})
                {%- endif %}
                {%- if copy_grants %}
                copy grants
                {%- endif %}
                {%- if row_access_policy is not none %}
                with row access policy {{ row_access_policy }}
                {%- endif %}
            as
                {%- if DDL == 'evolve schema' %}
                select * from {{ temp_relation }}
                {%- elif checksum is none and persist_strategy in ['delete+insert', 'merge'] %}
                select hash(*) as {{ all_checksums[0] }}, * from ({{ sql }})
                {%- else %}
                {{ sql }}
                {%- endif %}
                {%- if cluster_by is not none %}
                order by
                    {{ cluster_by | join(', ') }}
                {%- endif %}
        {% endcall %}

    {% elif persist_strategy == 'delete+insert' %}
        {% call statement('insert_delta') %}
            create or replace temporary table {{ delta_relation }} as
                select
                    source.*
                from
                    {{ temp_relation }} as source
                inner join
                    {{ delta_keys_relation }} as delta
                        on delta.{{ adapter.quote('persistent$source_row_count') }} is not null
                        {%- for column in incremental_keys %}
                        and source.{{ column }} is not distinct from delta.{{ column }}
                        {%- endfor %}
                {%- if cluster_by is not none %}
                order by
                    {{ cluster_by | join(', ') }}
                {%- endif %}
        {% endcall %}

        {% call statement('run_deletes') %}
            delete from
                {{ target_relation }} as destination
            using
                {{ delta_keys_relation }} as source
            where
                source.{{ adapter.quote('persistent$destination_row_count') }} is not null
                {%- for column in incremental_keys %}
                and destination.{{ column }} is not distinct from source.{{ column }}
                {%- endfor %}
        {% endcall %}

        {% call statement('main') %}
            {{ sql_header if sql_header is not none }}

            insert into {{ target_relation }}(
                {%- for column in adapter.get_columns_in_relation(delta_relation) %}
                {{ adapter.quote(column.name) }} {{- ',' if not loop.last }}
                {%- endfor %}
            )
                select * from {{ delta_relation }}
        {% endcall %}

        {% call statement('drop_delta') %}
            drop table if exists {{ delta_relation }}
        {% endcall %}

        {% call statement('drop_delta_keys') %}
            drop table if exists {{ delta_keys_relation }}
        {% endcall %}

    {% elif persist_strategy == 'merge' %}
        {% call statement('merge_delta') %}
            create or replace temporary table {{ delta_keys_relation }} as
                with
                    inserts as (
                        select
                            source.*,
                            'insert' as {{ adapter.quote('persistent$merge_action') }}
                        from
                            {{ temp_relation }} as source
                        inner join
                            {{ delta_keys_relation }} as delta
                                on delta.{{ adapter.quote('persistent$source_row_count') }} is not null
                                {%- for column in incremental_keys %}
                                and source.{{ column }} is not distinct from delta.{{ column }}
                                {%- endfor %}
                    ),

                    deletes as (
                        select
                            destination.*,
                            'delete' as {{ adapter.quote('persistent$merge_action') }}
                        from
                            {{ target_relation }} as destination
                        inner join
                            {{ delta_keys_relation }} as delta
                                on delta.{{ adapter.quote('persistent$source_row_count') }} is null
                                {%- for column in incremental_keys %}
                                and destination.{{ column }} is not distinct from delta.{{ column }}
                                {%- endfor %}
                    ),

                    merged as (
                        select * from inserts
                        union all
                        select * from deletes
                    )

                select * from merged
        {% endcall %}

        {% set columns = adapter.get_columns_in_relation(target_relation) %}

        {% call statement('main') %}
            {{ sql_header if sql_header is not none }}

            merge into
                {{ target_relation }} as destination
            using
                {%- if cluster_by is none %}
                {{ delta_keys_relation }} as delta
                {%- else %}
                (select * from {{ delta_keys_relation }} order by {{ cluster_by | join(', ') }}) as delta
                {%- endif %}
            on
                {%- for column in incremental_keys %}
                {{ "and " if not loop.first -}} destination.{{ column }} is not distinct from delta.{{ column }}
                {%- endfor %}
            when not matched and delta.{{ adapter.quote('persistent$merge_action') }} = 'insert' then
                insert (
                    {%- for column in columns %}
                    {{ adapter.quote(column.name) }} {{- ',' if not loop.last }}
                    {%- endfor %}
                ) values (
                    {%- for column in columns %}
                    delta.{{ adapter.quote(column.name) }} {{- ',' if not loop.last }}
                    {%- endfor %}
                )
            when matched and delta.{{ adapter.quote('persistent$merge_action') }} = 'insert' then
                update set
                    {%- for column in columns %}
                    {{ adapter.quote(column.name) }} = delta.{{ adapter.quote(column.name) }} {{- ',' if not loop.last }}
                    {%- endfor %}
            when matched and delta.{{ adapter.quote('persistent$merge_action') }} = 'delete' then
                delete
        {% endcall %}

        {% call statement('drop_delta_keys') %}
            drop table if exists {{ delta_keys_relation }}
        {% endcall %}

    {% elif persist_strategy == 'delete_only' %}
        {% call statement('main') %}
            delete from
                {{ target_relation }} as destination
            using
                {{ delta_keys_relation }} as source
            where
                {%- for column in incremental_keys %}
                {{ "and " if not loop.first -}} destination.{{ column }} is not distinct from source.{{ column }}
                {%- endfor %}
        {% endcall %}

        {% call statement('drop_delta_keys') %}
            drop table if exists {{ delta_keys_relation }}
        {% endcall %}

    {% elif persist_strategy == 'insert_only' %}
        {% call statement('main') %}
            {{ sql_header if sql_header is not none }}

            insert into {{ target_relation }}(
                {%- for column in adapter.get_columns_in_relation(temp_relation) %}
                {{ adapter.quote(column.name) }} {{- ',' if not loop.last }}
                {%- endfor %}
            )
                select * from (
                    select
                        source.*
                    from
                        {{ temp_relation }} as source
                    inner join
                        {{ delta_keys_relation }} as delta
                            {%- for column in incremental_keys %}
                            {{ "on" if loop.first else "and" }} source.{{ column }} is not distinct from delta.{{ column }}
                            {%- endfor %}
                )
                {%- if cluster_by is not none %}
                order by
                    {{ cluster_by | join(', ') }}
                {%- endif %}
        {% endcall %}

        {% call statement('drop_delta_keys') %}
            drop table if exists {{ delta_keys_relation }}
        {% endcall %}

    {% elif persist_strategy == 'insert_overwrite' %}
        {% call statement('main') %}
            insert overwrite into {{ target_relation }}(
                {%- for column in adapter.get_columns_in_relation(temp_relation) %}
                {{ adapter.quote(column.name) }} {{- ',' if not loop.last }}
                {%- endfor %}
            )
                select * from {{ temp_relation }}
                {%- if cluster_by is not none %}
                order by
                    {{ cluster_by | join(', ') }}
                {%- endif %}
        {% endcall %}

    {% endif %}

    {% do log(DDL) %}
    {% do log(drop_result) %}
    {% do log(alter_if) %}
    {% do log(cluster_by) %}

    {% set alter_table %}
        with alter_table as procedure()
            returns table()
        as $$
            begin
                {%- if DDL == 'alter if exists' %}
                {%- for part in drop_result['alter_if'] %}
                {%- if part.startswith('Row Access Policy') %}
                alter table if exists {{ target_relation }} drop all row access policies;
                alter table if exists {{ target_relation }} add row access policy {{ row_access_policy }};
                {%- elif part.startswith('Change Tracking') %}
                alter table if exists {{ target_relation }} set change_tracking = true;
                {%- elif part.startswith('Cluster By') and cluster_by %}
                alter table if exists {{ target_relation }} cluster by ({{ cluster_by | join(', ') }});
                {%- endif %}
                {%- endfor %}
                {%- elif DDL in ['create or replace', 'evolve schema'] and change_tracking %}
                alter table if exists {{ target_relation }} set change_tracking = true;
                {%- endif %}
                {%- if DDL != 'create if not exists' and alter_if %}
                alter table if exists {{ target_relation }} set comment = '{{ alter_if | join('\\n') }}';
                {%- endif %}
                let res resultset := (
                    drop {{ tmp_relation_type }} if exists {{ temp_relation }}
                );
                return table(res);
            end
        $$

        call alter_table()
    {% endset %}

    {{ run_hooks(post_hooks, inside_transaction=true) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}
    {{ run_hooks(post_hooks, inside_transaction=false) }}

    {% if config.get('grants') is not none %}
        {% do custom_apply_grants(target_relation, config.get('grants')) %}
    {% endif %}

    {% if config.persist_relation_docs() %}
        {% do custom_persist_docs(target_relation, model, 'table', (alter_if | join('\\n'))) %}
    {% endif %}

    {% do unset_query_tag(original_query_tag) %}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
