{% materialization merge_on_read, adapter='snowflake', supported_languages=['sql'] %}
    {% set original_query_tag = set_query_tag() %}
    {% set sql_header = config.get('sql_header') %}

    {% set transient = config.get('transient', true) %}

    {% if transient %}
        {% set alter_if = ['Transient: true'] %}
    {% else %}
        {% set alter_if = ['Transient: false'] %}
    {% endif %}

    {% set copy_grants = config.get('copy_grants', false) %}

    {% set row_access_policy = config.get('row_access_policy') %}

    {% if row_access_policy is not none %}
        {% set row_access_policy = parse_jinja(row_access_policy|string)['code'] %}
    {% endif %}

    {% set unique_key = config.get('unique_key') %}
    {% set checksum = config.get('check_cols') %}
    {% set change_tracking = config.get('change_tracking') %}

    {% if change_tracking is not none %}
        {% do alter_if.append('Change Tracking: ' ~ (change_tracking | string)) %}
    {% endif %}

    {% set tmp_relation_type = config.get('tmp_relation_type', 'view') %}
    {% set cluster_by = config.get('cluster_by') %}

    {% if cluster_by is not none %}
        {% set cluster_by_hash = local_md5(cluster_by|string) %}
        {% do alter_if.append('Cluster By Hash: ' ~ cluster_by_hash) %}
    {% endif %}

    {% set partition_by = config.get('partition_by') %}

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

    {% if all_keys == [] %}
        {% set any_keys = all_checksums %}
    {% else %}
        {% set any_keys = all_keys %}
    {% endif %}

    {% set target_relation = get_fully_qualified_relation(this).incorporate(type='view') %}
    {% set temp_relation = get_fully_qualified_relation(make_temp_relation(target_relation).incorporate(type=tmp_relation_type)) %}

    {% if target_relation.identifier.endswith('"') %}
        {% set history_identifier = target_relation.identifier[:-1] ~ '_HISTORY"' %}
        {% set deletes_identifier = target_relation.identifier[:-1] ~ '_DELETES"' %}
        {% set inserts_identifier = target_relation.identifier[:-1] ~ '_INSERTS"' %}
    {% else %}
        {% set history_identifier = target_relation.identifier ~ '_HISTORY' %}
        {% set deletes_identifier = target_relation.identifier ~ '_DELETES' %}
        {% set inserts_identifier = target_relation.identifier ~ '_INSERTS' %}
    {% endif %}

    {% set history_relation = target_relation.incorporate(path={'identifier': history_identifier}, type='table') %}
    {% set deletes_relation = target_relation.incorporate(path={'identifier': deletes_identifier}, type='table') %}
    {% set inserts_relation = target_relation.incorporate(path={'identifier': inserts_identifier}, type='table') %}

    {% if alter_if == [] %}
        {% set alter_if = none %}
    {% endif %}

    {% set drop_result = drop_relation_unless(target_relation, 'view', alter_if=alter_if) %}

    {% set DDL = drop_result['DDL'] %}

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=false) }}

    -- BEGIN happens here:
    {{ run_hooks(pre_hooks, inside_transaction=true) }}

    --------------------------------------------------------------------------------------------------------------------
    -- build model

    {% if DDL == 'create or replace' %}
        {% do drop_relation_unless(inserts_relation, 'table', none, true) %}
        {% do drop_relation_unless(deletes_relation, 'table', none, true) %}
        {% do drop_relation_unless(history_relation, 'table', none, transient) %}

        {% call statement('main') %}
            {{ sql_header if sql_header is not none }}

            create or replace transient table {{ inserts_relation }}
                {%- if copy_grants %}
                copy grants
                {%- endif %}
            as
                {%- if checksum is none %}
                select hash(*) as {{ all_checksums[0] }}, * from ({{ sql }})
                {%- else %}
                {{ sql }}
                {%- endif %}
                {%- if cluster_by is not none %}
                order by {{ cluster_by | join(', ') }}
                {%- elif partition_by is not none %}
                order by {{ partition_by | join(', ') }}
                {%- endif %}
        ->>
            create or replace transient table {{ deletes_relation }}
                {%- if copy_grants %}
                copy grants
                {%- endif %}
            as
                select
                    {%- for column in any_keys %}
                    {{ column }} {{- ',' if not loop.last }}
                    {%- endfor %}
                from
                    {{ inserts_relation }}
                where
                    false
        ->>
            create or replace {{- ' transient' if transient }} table {{ history_relation }}
                {%- if cluster_by is not none %}
                cluster by ({{ cluster_by | join(', ') }})
                {%- endif %}
                {%- if copy_grants %}
                copy grants
                {%- endif %}
            as
                select
                    *
                from
                    {{ inserts_relation }}
                where
                    false
        ->>
            create or replace view {{ target_relation }}
                {%- if copy_grants %}
                copy grants
                {%- endif %}
                {%- if alter_if is not none %}
                comment = '{{ alter_if | join("\\n") }}'
                {%- endif %}
                {%- if row_access_policy is not none %}
                with row access policy {{ row_access_policy }}
                {%- endif %}
            as
                select
                    history.*
                from
                    {{ history_relation }} as history
                left join
                    (
                        select
                            *,
                            1 as {{ adapter.quote("temp$flag") }}
                        from
                            {{ deletes_relation }}
                    ) as deletes
                        {%- for column in any_keys %}
                        {% if loop.first -%} on {% else -%} and {% endif -%}
                        deletes.{{ column }} is not distinct from history.{{ column }}
                        {%- endfor %}
                where
                    deletes.{{ adapter.quote("temp$flag") }} is null

                union all

                select * from {{ inserts_relation }}
        {% endcall %}

        {{ return({'relations': [this.incorporate(type='view'), history_relation, deletes_relation, inserts_relation]}) }}
    {% endif %}

    {% call statement('temp_relation') %}
        {{ sql_header if sql_header is not none }}

        create or replace temporary {{ tmp_relation_type }} {{ temp_relation }} as
            {%- if checksum is none %}
            select hash(*) as {{ all_checksums[0] }}, * from ({{ sql }})
            {%- else %}
            {{ sql }}
            {%- endif %}
    {% endcall %}

    {% if evolve_schema(temp_relation, target_relation, transient) != [] %}
        {% do drop_relation_unless(inserts_relation, 'table', none, true) %}
        {% do drop_relation_unless(deletes_relation, 'table', none, true) %}
        {% do drop_relation_unless(history_relation, 'table', none, transient) %}

        {% call statement('main') %}
            {{ sql_header if sql_header is not none }}

            create or replace transient table {{ inserts_relation }}
                {%- if copy_grants %}
                copy grants
                {%- endif %}
            as
                select * from {{ temp_relation }}
                {%- if cluster_by is not none %}
                order by {{ cluster_by | join(', ') }}
                {%- elif partition_by is not none %}
                order by {{ partition_by | join(', ') }}
                {%- endif %}
        ->>
            create or replace transient table {{ deletes_relation }}
                {%- if copy_grants %}
                copy grants
                {%- endif %}
            as
                select
                    {%- for column in any_keys %}
                    {{ column }} {{- ',' if not loop.last }}
                    {%- endfor %}
                from
                    {{ inserts_relation }}
                where
                    false
        ->>
            create or replace {{- ' transient' if transient }} table {{ history_relation }}
                {%- if cluster_by is not none %}
                cluster by ({{ cluster_by | join(', ') }})
                {%- endif %}
                {%- if copy_grants %}
                copy grants
                {%- endif %}
            as
                select
                    *
                from
                    {{ inserts_relation }}
                where
                    false
        ->>
            create or replace view {{ target_relation }}
                {%- if copy_grants %}
                copy grants
                {%- endif %}
                {%- if alter_if is not none %}
                comment = '{{ alter_if | join("\\n") }}'
                {%- endif %}
                {%- if row_access_policy is not none %}
                with row access policy {{ row_access_policy }}
                {%- endif %}
            as
                select
                    history.*
                from
                    {{ history_relation }} as history
                left join
                    (
                        select
                            *,
                            1 as {{ adapter.quote("temp$flag") }}
                        from
                            {{ deletes_relation }}
                    ) as deletes
                        {%- for column in any_keys %}
                        {% if loop.first -%} on {% else -%} and {% endif -%}
                        deletes.{{ column }} is not distinct from history.{{ column }}
                        {%- endfor %}
                where
                    deletes.{{ adapter.quote("temp$flag") }} is null

                union all

                select * from {{ inserts_relation }}
        ->>
            drop table if exists {{ temp_relation }}
        {% endcall %}

        {{ return({'relations': [this.incorporate(type='view'), history_relation, deletes_relation, inserts_relation]}) }}
    {% endif %}

    {% set alter_transient = [] %}

    {% if DDL == 'alter if exists' and 'alter_if' in drop_result %}
        {% for part in drop_result['alter_if'] if part.startswith('Transient') %}
            {% do alter_transient.append(true) %}
        {% endfor %}
    {% endif %}

    {% if partition_by is none or partition_by == [] %}
        {% set delta_relation = get_fully_qualified_relation(make_temp_relation(temp_relation).incorporate(type='table')) %}

        {% call statement('main') %}
            {%- if alter_transient == [] %}
            {%- if DDL == 'alter if exists' and 'alter_if' in drop_result and cluster_by == [] %}
            {%- for part in drop_result['alter_if'] if part.startswith('Cluster By') %}
            alter table {{ history_relation }} drop clustering key
        ->>
            {%- endfor %}
            {%- endif %}
            delete from
                {{ history_relation }} as target
            using
                {{ deletes_relation }} as deletes
            where
                {%- for column in adapter.get_columns_in_relation(deletes_relation) %}
                {{ 'and ' if not loop.first -}} target.{{ adapter.quote(column.name) }} is not distinct from deletes.{{ adapter.quote(column.name) }}
                {%- endfor %}
        ->>
            truncate {{ deletes_relation }}
        ->>
            insert into {{ history_relation }}
                select * from {{ inserts_relation }}
        ->>
            truncate {{ inserts_relation }}
        ->>
            {%- if DDL == 'alter if exists' and 'alter_if' in drop_result and cluster_by != [] %}
            {%- for part in drop_result['alter_if'] if part.startswith('Cluster By') %}
            alter table {{ history_relation }} cluster by ({{ cluster_by | join(', ') }})
        ->>
            {%- endfor %}
            {%- endif %}
            {%- else %}
            create or replace {{- ' transient' if transient }} table {{ history_relation }}
                {%- if cluster_by is not none %}
                cluster by ({{ cluster_by | join(', ') }})
                {%- endif %}
                {%- if copy_grants %}
                copy grants
                {%- endif %}
            as
                select * from {{ target_relation }}
        ->>
            truncate {{ deletes_relation }}
        ->>
            truncate {{ inserts_relation }}
        ->>
            {%- endif %}
            create or replace temporary table {{ delta_relation }} as
                with
                    source as (select *, 1 as {{ adapter.quote("source$flag") }} from {{ temp_relation }}),
                    history as (select *, 1 as {{ adapter.quote("history$flag") }} from {{ history_relation }})

                select distinct
                    {%- for column in any_keys %}
                    ifnull(source.{{ column }}, history.{{ column }}) as {{ column }},
                    {%- endfor %}
                    source.{{ adapter.quote("source$flag") }},
                    history.{{ adapter.quote("history$flag") }}
                from
                    source
                full outer join
                    history
                        {%- for column in array_union(all_keys, all_checksums) %}
                        {{ 'on ' if loop.first else 'and ' -}}
                        source.{{ column }} is not distinct from history.{{ column }}
                        {%- endfor %}
                where
                    source.{{ adapter.quote("source$flag") }} is distinct from history.{{ adapter.quote("history$flag") }}
        ->>
            create or replace transient table {{ inserts_relation }}
                {%- if copy_grants %}
                copy grants
                {%- endif %}
            as
                select
                    source.*
                from
                    {{ temp_relation }} as source
                inner join
                    {{ delta_relation }} as delta
                        on delta.{{ adapter.quote("source$flag") }} = 1
                        {%- for column in any_keys %}
                        and source.{{ column }} is not distinct from delta.{{ column }}
                        {%- endfor %}
        ->>
            create or replace transient table {{ deletes_relation }}
                {%- if copy_grants %}
                copy grants
                {%- endif %}
            as
                select
                    * exclude (
                        {{ adapter.quote("source$flag") }},
                        {{ adapter.quote("history$flag") }}
                    )
                from
                    {{ delta_relation }}
                where
                    {{ adapter.quote("history$flag") }} = 1
        ->>
            create or replace view {{ target_relation }}
                {%- if copy_grants %}
                copy grants
                {%- endif %}
                {%- if alter_if is not none %}
                comment = '{{ alter_if | join("\\n") }}'
                {%- endif %}
                {%- if row_access_policy is not none %}
                with row access policy {{ row_access_policy }}
                {%- endif %}
            as
                select
                    history.*
                from
                    {{ history_relation }} as history
                left join
                    (
                        select
                            *,
                            1 as {{ adapter.quote("temp$flag") }}
                        from
                            {{ deletes_relation }}
                    ) as deletes
                        {%- for column in any_keys %}
                        {% if loop.first -%} on {% else -%} and {% endif -%}
                        deletes.{{ column }} is not distinct from history.{{ column }}
                        {%- endfor %}
                where
                    deletes.{{ adapter.quote("temp$flag") }} is null

                union all

                select * from {{ inserts_relation }}
        ->>
            drop {{ tmp_relation_type }} if exists {{ temp_relation }}
        ->>
            drop table if exists {{ delta_relation }}
        {% endcall %}
    {% else %}
        {% set partition_query %}
            select
                *, hash(*)
            from
                (
                    select {{ partition_by | join(', ') }} from {{ history_relation }}
                    union
                    select {{ partition_by | join(', ') }} from {{ temp_relation }}
                )
        {% endset %}

        {% set partitions = run_query(partition_query) %}

        {% set delta_relations = {} %}

        {% for partition in partitions %}
            {% if temp_relation.endswith('"') %}
                {% do delta_relations.update({partition[-1]: temp_relation.incorporate(path={'identifier': temp_relation.identifier[:-1] ~ '__DELTA_' ~ partition[-1] ~ '"'})}) %}
            {% else %}
                {% do delta_relations.update({partition[-1]: temp_relation.incorporate(path={'identifier': temp_relation.identifier ~ '__DELTA_' ~ partition[-1]})}) %}
            {% endif %}
        {% endfor %}

        {% set main_query %}
            with main_query as procedure()
                returns table()
            as $$
                begin
                    {%- if alter_transient == [] %}
                    {%- if DDL == 'alter if exists' and 'alter_if' in drop_result and cluster_by == [] %}
                    {%- for part in drop_result['alter_if'] if part.startswith('Cluster By') %}
                    alter table {{ history_relation }} drop clustering key;
                    {%- endfor %}
                    {%- endif %}

                    delete from
                        {{ history_relation }} as target
                    using
                        {{ deletes_relation }} as deletes
                    where
                        {%- for column in adapter.get_columns_in_relation(deletes_relation) %}
                        {{ 'and ' if not loop.first -}} target.{{ adapter.quote(column.name) }} is not distinct from deletes.{{ adapter.quote(column.name) }}
                        {%- endfor %};

                    async (
                        insert into {{ history_relation }}
                            select * from {{ inserts_relation }}
                    );

                    truncate {{ deletes_relation }};
                    await all;
                    truncate {{ inserts_relation }};

                    {%- if DDL == 'alter if exists' and 'alter_if' in drop_result and cluster_by != [] %}
                    {%- for part in drop_result['alter_if'] if part.startswith('Cluster By') %}
                    alter table {{ history_relation }} cluster by ({{ cluster_by | join(', ') }});
                    {%- endfor %}
                    {%- endif %}
                    {%- else %}

                    create or replace {{- ' transient' if transient }} table {{ history_relation }}
                        {%- if cluster_by is not none %}
                        cluster by ({{ cluster_by | join(', ') }})
                        {%- endif %}
                        {%- if copy_grants %}
                        copy grants
                        {%- endif %}
                    as
                        select * from {{ target_relation }};

                    truncate {{ deletes_relation }};
                    truncate {{ inserts_relation }};

                    {%- endif %}

                    {%- for partition in partitions %}

                    async (
                        create or replace temporary table {{ delta_relations[partition[-1]] }} as
                            with
                                source as (
                                    select
                                        *,
                                        1 as {{ adapter.quote("source$flag") }}
                                    from
                                        {{ temp_relation }}
                                    where
                                        {%- for column, value in zip(partition_by, partition) %}
                                        {%- if value is none %}
                                        {{ column }} is none
                                        {%- elif value is string %}
                                        {{ column }} = '{{ value.replace("\\", "\\\\").replace("'", "\\'") }}'
                                        {%- else %}
                                        {{ column }} = {{ value }}
                                        {%- endif %}
                                        {%- endfor %}
                                ),

                                history as (
                                    select
                                        *,
                                        1 as {{ adapter.quote("history$flag") }}
                                    from
                                        {{ history_relation }}
                                    where
                                        {%- for column, value in zip(partition_by, partition) %}
                                        {%- if value is none %}
                                        {{ column }} is none
                                        {%- elif value is string %}
                                        {{ column }} = '{{ value.replace("\\", "\\\\").replace("'", "\\'") }}'
                                        {%- else %}
                                        {{ column }} = {{ value }}
                                        {%- endif %}
                                        {%- endfor %}
                                )

                            select distinct
                                {%- for column in any_keys %}
                                ifnull(source.{{ column }}, history.{{ column }}) as {{ column }},
                                {%- endfor %}
                                source.{{ adapter.quote("source$flag") }},
                                history.{{ adapter.quote("history$flag") }}
                            from
                                source
                            full outer join
                                destination
                                    {%- for column in array_union(all_keys, all_checksums) %}
                                    {{ 'on ' if loop.first else 'and ' -}}
                                    source.{{ column }} is not distinct from history.{{ column }}
                                    {%- endfor %}
                            where
                                source.{{ adapter.quote("source$flag") }} is distinct from history.{{ adapter.quote("history$flag") }}
                    );

                    {%- endfor %}

                    await all;

                    async (
                        create or replace transient table {{ inserts_relation }} as
                            select
                                source.*
                            from
                                {{ temp_relation }} as source
                            inner join
                                (
                                    {%- for delta_relation in delta_relations.values() %}
                                    select * from {{ delta_relation }}
                                    {%- if not loop.last %}
                                    union all
                                    {%- endif %}
                                    {%- endfor %}
                                ) as delta
                                    on delta.{{ adapter.quote("source$flag") }} = 1
                                    {%- for column in any_keys %}
                                    and source.{{ column }} is not distinct from delta.{{ column }}
                                    {%- endfor %}
                    );

                    create or replace transient table {{ deletes_relation }} as
                        select
                            * exclude (
                                {{ adapter.quote("source$flag") }},
                                {{ adapter.quote("history$flag") }}
                            )
                        from
                            (
                                {%- for delta_relation in delta_relations.values() %}
                                select * from {{ delta_relation }}
                                {%- if not loop.last %}
                                union all
                                {%- endif %}
                                {%- endfor %}
                            )
                        where
                            {{ adapter.quote("history$flag") }} = 1;

                    await all;

                    let res resultset := (
                        create or replace view {{ target_relation }}
                            {%- if copy_grants %}
                            copy grants
                            {%- endif %}
                            {%- if alter_if is not none %}
                            comment = '{{ alter_if | join("\\n") }}'
                            {%- endif %}
                            {%- if row_access_policy is not none %}
                            with row access policy {{ row_access_policy }}
                            {%- endif %}
                        as
                            select
                                history.*
                            from
                                {{ history_relation }} as history
                            left join
                                (
                                    select
                                        *,
                                        1 as {{ adapter.quote("temp$flag") }}
                                    from
                                        {{ deletes_relation }}
                                ) as deletes
                                    {%- for column in any_keys %}
                                    {% if loop.first -%} on {% else -%} and {% endif -%}
                                    deletes.{{ column }} is not distinct from history.{{ column }}
                                    {%- endfor %}
                            where
                                deletes.{{ adapter.quote("temp$flag") }} is null

                            union all

                            select * from {{ inserts_relation }}
                    );

                    {%- for delta_relation in delta_relations.values() %}
                    async (drop table if exists {{ delta_relation }});
                    {%- endfor %}
                    drop {{ tmp_relation_type }} if exists {{ temp_relation }};

                    await all;

                    return table(res);
                end
            $$

            call main_query()
        {% endset %}

        {% call statement('main') %}
            {{ sql_run_safe(main_query) }}
        {% endcall %}
    {% endif %}

    {{ run_hooks(post_hooks, inside_transaction=true) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}
    {{ run_hooks(post_hooks, inside_transaction=false) }}

    {% if config.get('grants') is not none %}
        {% do custom_apply_grants(target_relation, config.get('grants')) %}
    {% endif %}

    {% if config.persist_relation_docs() %}
        {% do custom_persist_docs(target_relation, model, 'view', (alter_if | join('\\n'))) %}
    {% endif %}

    {% do unset_query_tag(original_query_tag) %}

    {{ return({'relations': [this.incorporate(type='view'), history_relation, deletes_relation, inserts_relation]}) }}
{% endmaterialization %}
