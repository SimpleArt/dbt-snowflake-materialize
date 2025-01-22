{% materialization persistent_table, adapter='snowflake', supported_languages=['sql'] %}
    {% set original_query_tag = set_query_tag() %}
    {% set grant_config = config.get('grants') %}
    {% set sql_header = config.get('sql_header') %}

    {% set transient = config.get('transient', false) %}
    {% set copy_grants = config.get('copy_grants', false) %}

    {% set persist_strategy = config.get('persist_strategy', 'insert_overwrite') %}
    {% set tmp_relation_type = config.get('tmp_relation_type', 'view') %}
    {$ set unique_key = config.get('unique_key') %}
    {% set checksum = config.get('check_cols', 'all') %}
    {% set cluster_by = config.get('cluster_by') %}

    {% set all_keys = [] %}

    {% if unique_key is string %}
        {% set all_keys = [unique_key] %}
    {% elif unique_key is none %}
        {% set all_keys = [] %}
    {% else %}
        {% set all_keys = array_unique(unique_key) %}
    {% endif %}

    {% if checksum == 'all' %}
        {% set all_checksums = [adapter.quote('PERSISTENT$CHECKSUM')]
    {% elif checksum is string %}
        {% set all_checksums = [checksum] %}
    {% else %}
        {% set all_checksums = array_unique(checksum) %}
    {% endif %}

    {% set target_relation = this.incorporate(type='table') %}
    {% set temp_relation = make_temp_relation(target_relation).incorporate(type=tmp_relation_type) %}
    {% set delta_keys_relation = make_temp_relation(temp_relation).incorporate(type='table') %}
    {% set delta_relation = make_temp_relation(delta_keys_relation).incorporate(type='table') %}

    {% set should_revoke = drop_relation(target_relation) %}

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=false) }}

    -- BEGIN happens here:
    {{ run_hooks(pre_hooks, inside_transaction=true) }}

    --------------------------------------------------------------------------------------------------------------------
    -- build model

    {% if should_revoke %}
        {% call statement('setup') %}
            create or replace temporary {{ tmp_relation_type }} {{ temp_relation }} as
                {%- if checksum == 'all' and persist_strategy in ['delete+insert', 'merge'] %}
                select hash(*) as {{ all_checksums[0] }}, * from ({{ sql }})
                {%- else %}
                {{ sql }}
                {%- endif %}
        {% endcall %}
    {% endif %}

    {% set table_count = 1 %}
    {% set schema_count = 2 %}

    {% if should_revoke and execute %}
        {% set compare_schema %}
            select
                count(*) as table_count,
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
                        table_catalog ilike $${{ target_relation.database }}$$
                        and table_schema ilike $${{ target_relation.schema }}$$
                        and table_name ilike any ($${{ target_relation.identifier }}$$, $${{ temp_relation.identifier }}$$)
                    group by
                        table_name
                )
        {% endset %}

        {% set row = run_query(compare_schema)[0] %}
        {% set table_count = row['TABLE_COUNT'] %}
        {% set schema_count = row['SCHEMA_COUNT'] %}
    {% endif %}

    {% if not should_revoke or not execute or schema_count == 2 or table_count == 1
        or persist_strategy not in ['delete+insert', 'insert_overwrite', 'merge']
    %}
        {% set persist_strategy = 'create_or_replace' %}

    {% elif persist_strategy in ['delete+insert', 'merge'] %}
        {% set union_keys = array_union(all_keys, all_checksums) %}

        {% call statement('delta_keys') %}
            create or replace temporary table {{ delta_keys_relation }} as
                with
                    source as (
                        select
                            {%- for column in union_keys %}
                            {{ column }},
                            {%- endfor %}
                            count(*) as {{ adapter.quote('PERSISTENT$ROW_COUNT') }}
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
                            count(*) as {{ adapter.quote('PERSISTENT$ROW_COUNT') }}
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
                            source.{{ adapter.quote('PERSISTENT$ROW_COUNT') }} as {{ adapter.quote('PERSISTENT$SOURCE_ROW_COUNT') }},
                            destination.{{ adapter.quote('PERSISTENT$ROW_COUNT') }} as {{ adapter.quote('PERSISTENT$DESTINATION_ROW_COUNT') }}
                        from
                            source
                        full outer join
                            destination
                                {%- for column in union_keys %}
                                {{ "on" if loop.first else "and" }} source.{{ column }} is not distinct from destination.{{ column }}
                                {%- endfor %}
                        where
                            source.{{ adapter.quote('PERSISTENT$ROW_COUNT') }} is distinct from destination.{{ adapter.quote('PERSISTENT$ROW_COUNT') }}
                    ),

                    {%- if all_keys == [] or persist_strategy == 'delete+insert' %}

                    final as (
                        select
                            *,
                            {{ adapter.quote('PERSISTENT$SOURCE_ROW_COUNT') }} as {{ adapter.quote('PERSISTENT$SOURCE_KEY_COUNT') }},
                            {{ adapter.quote('PERSISTENT$DESTINATION_ROW_COUNT') }} as {{ adapter.quote('PERSISTENT$DESTINATION_KEY_COUNT') }}
                        from
                            delta
                    )

                    {%- else %}

                    source_counts as (
                        select
                            {%- for column in all_keys %}
                            {{ column }},
                            {%- endfor %}
                            sum({{ adapter.quote('PERSISTENT$ROW_COUNT') }}) as {{ adapter.quote('PERSISTENT$ROW_COUNT') }}
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
                            sum({{ adapter.quote('PERSISTENT$ROW_COUNT') }}) as {{ adapter.quote('PERSISTENT$ROW_COUNT') }}
                        from
                            destination
                        group by
                            all
                    ),

                    final as (
                        select
                            delta.*,
                            source_counts.{{ adapter.quote('PERSISTENT$ROW_COUNT') }} as {{ adapter.quote('PERSISTENT$SOURCE_KEY_COUNT') }},
                            destination_counts.{{ adapter.quote('PERSISTENT$ROW_COUNT') }} as {{ adapter.quote('PERSISTENT$DESTINATION_KEY_COUNT') }}
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
                                {{ "on" if loop.first else "and" }} delta.{{ column }} is not distinct from source_counts.{{ column }}
                                {%- endfor %}
                    )

                    {%- endif %}

                select * from final
        {% endcall %}

        {% set stats_query %}
            select
                zeroifnull(max({{ adapter.quote('PERSISTENT$SOURCE_ROW_COUNT') }})) as source_row_count,
                zeroifnull(max({{ adapter.quote('PERSISTENT$SOURCE_KEY_COUNT') }})) as source_key_count,
                zeroifnull(max({{ adapter.quote('PERSISTENT$DESTINATION_ROW_COUNT') }})) as destination_row_count,
                zeroifnull(max({{ adapter.quote('PERSISTENT$DESTINATION_KEY_COUNT') }})) as destination_key_count,
                zeroifnull(sum({{ adapter.quote('PERSISTENT$SOURCE_ROW_COUNT') }})) as inserts,
                zeroifnull(sum({{ adapter.quote('PERSISTENT$DESTINATION_ROW_COUNT') }})) as deletes
            from
                {{ delta_keys_relation }}
        {% endset %}

        {% set incremental_keys = union_keys %}

        {% if persist_strategy == 'merge' %}
            {% set row = run_query(stats_query)[0] %}

            {% if row['INSERTS'] == 0 %}
                {% set persist_strategy = 'delete_only' %}
            {% elif row['DELETES'] == 0 %}
                {% set persist_strategy = 'insert_only' %}
            {% elif row['SOURCE_ROW_COUNT'] > 1 or row['DESTINATION_ROW_COUNT'] > 1 %}
                {% set persist_strategy = 'delete+insert' %}
            {% elif all_keys != [] and row['SOURCE_KEY_COUNT'] <= 1 and row['DESTINATION_KEY_COUNT'] <= 1 %}
                {% set incremental_keys = all_keys %}
            {% endif %}
        {% endif %}

    {% endif %}

    {% if persist_strategy == 'delete+insert' %}
        {% call statement('insert_delta') %}
            create or replace temporary table {{ delta_relation }} as
                select
                    source.*
                from
                    {{ temp_relation }} as source
                inner join
                    {{ delta_keys_relation }} as delta
                        on delta.{{ adapter.quote('PERSISTENT$SOURCE_ROW_COUNT') }} is not null
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
                {%- for column in incremental_keys %}
                {{ "and " if loop.first -}} destination.{{ column }} is not distinct from source.{{ column }}
                {%- endfor %}
        {% endcall %}

        {% call statement('main') %}
            {{ sql_header if sql_header is not none }}

            insert into {{ target_relation }}
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
                            'insert' as {{ adapter.quote('PERSISTENT$MERGE_ACTION') }}
                        from
                            {{ temp_relation }} as source
                        inner join
                            {{ delta_keys_relation }} as delta
                                on delta.{{ adapter.quote('PERSISTENT$SOURCE_ROW_COUNT') }} is not null
                                {%- for column in incremental_keys %}
                                and source.{{ column }} is not distinct from delta.{{ column }}
                                {%- endfor %}
                    ),

                    deletes as (
                        select
                            destination.*,
                            'delete' as {{ adapter.quote('PERSISTENT$MERGE_ACTION') }}
                        from
                            {{ target_relation }} as destination
                        inner join
                            {{ delta_keys_relation }} as delta
                                on delta.{{ adapter.quote('PERSISTENT$DESTINATION_ROW_COUNT') }} is not null
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
                {{ "and " if not loop.first -}} destination.{{ column }} is distinct from delta.{{ column }}
                {%- endif %}
            when not matched and delta.{{ adapter.quote('PERSISTENT$MERGE_ACTION') }} = 'insert' then
                insert (
                    {%- for column in columns %}
                    {{ adapter.quote(column.name) }} {{- ',' if not loop.last }}
                    {%- endfor %}
                ) values (
                    {%- for column in columns %}
                    delta.{{ adapter.quote(column.name) }} {{- ',' if not loop.last }}
                    {%- endfor %}
                )
            when matched and delta.{{ adapter.quote('PERSISTENT$MERGE_ACTION') }} = 'insert' then
                update set
                    {%- for column in columns %}
                    {{ adapter.quote(column.name) }} = delta.{{ adapter.quote(column.name) }} {{- ',' if not loop.last }}
                    {%- endfor %}
            when matched and delta.{{ adapter.quote('PERSISTENT$MERGE_ACTION') }} = 'delete' then
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
                {{ "and " if loop.first -}} destination.{{ column }} is not distinct from source.{{ column }}
                {%- endfor %}
        {% endcall %}

        {% call statement('drop_delta_keys') %}
            drop table if exists {{ delta_keys_relation }}
        {% endcall %}

    {% elif persist_strategy == 'insert_only' %}
        {% call statement('main') %}
            {{ sql_header if sql_header is not none }}

            insert into {{ target_relation }}
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
            insert overwrite into {{ target_relation }}
                select * from {{ temp_relation }}
                {%- if cluster_by is not none %}
                order by
                    {{ cluster_by | join(', ') }}
                {%- endif %}
        {% endcall %}

    {% else %}
        {% call statement('main') %}
            {{ sql_header if sql_header is not none }}

            create or replace {{- ' transient' if transient }} table {{ target_relation }}
                {%- if copy_grants %}
                copy grants
                {%- endif %}
                {%- if cluster_by is not none %}
                cluster by ({{ cluster_by | join(', ') }})
                {%- endif %}
            as
                {%- if should_revoke %}
                select * from {{ temp_relation }}
                {%- elif checksum is none and persist_strategy = 'incremental' %}
                select hash(*) as {{ all_checksums[0] }}, * from ({{ sql }})
                {%- else %}
                {{ sql }}
                {%- endif %}
                {%- if cluster_by is not none %}
                order by
                    {{ cluster_by | join(', ') }}
                {%- endif %}
        {% endcall %}

    {% endif %}

    {% if should_revoke %}
        {% call statement('drop_temp') %}
            drop {{ tmp_relation_type }} if exists {{ temp_relation }}
        {% endcall %}
    {% endif %}

    {% if cluster_by is not none %}
        {% call statement('set_clustering_key') %}
            alter table {{ target_relation }}
                cluster by ({{ cluster_by | join(', ') }})
        {% endcall %}
    {% endif %}

    {{ run_hooks(post_hooks, inside_transaction=true) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}
    {{ run_hooks(post_hooks, inside_transaction=false) }}

    {% if config.get('grants') is not none %}
        {% do post_apply_grants(target_relation, config.get('grants'), should_revoke) %}
    {% endif %}

    {% if config.persist_relation_docs() %}
        {% do post_persist_docs(target_relation, model) %}
    {% endif %}

    {% do unset_query_tag(original_query_tag) %}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
