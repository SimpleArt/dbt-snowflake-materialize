{% materialization insert_overwrite, adapter='snowflake', supported_languages=['sql'] %}
    {% set original_query_tag = set_query_tag() %}
    {% set sql_header = config.get('sql_header') %}

    {% set tmp_relation_type = config.get('tmp_relation_type') %}
    {% set partition_by = config.get('partition_by') %}

    {% if tmp_relation_type not in ['table', 'view'] %}
        {% do exceptions.warn("Invalid `tmp_relation_type`, expected string 'table | view', got " ~ tojson(tmp_relation_type)) %}
        {% set tmp_relation_type = 'view' %}
    {% endif %}

    {% if has_keys(partition_by, 'from') and partition_by.get('columns') is iterable %}
        {% set temp = {'from': compile_config(partition_by.get('from')), 'columns': join_strings(partition_by.get('columns'))} %}
        {% if partition_by.get('partitions') is integer and partition_by.get('partitions') > 1 %}
            {% do temp.update({'partitions': partition_by.get('partitions')}) %}
        {% elif partition_by is not none %}
            {% do exceptions.warn("Invalid `partition_by.partitions`, expected int > 1, got " ~ tojson(partition_by.get('partitions'))) %}
        {% endif %}
        {% if partition_by.get('async') is boolean %}
            {% do temp.update({'async': partition_by.get('async')}) %}
        {% elif partition_by.get('async') is not none %}
            {% do exceptions.warn("Invalid `partition_by.async`, expected bool <true | false>, got " ~ tojson(partition_by.get('async'))) %}
        {% endif %}
        {% set partition_by = temp %}
    {% elif partition_by is not none %}
        {% do exceptions.warn("Invalid `partition_by`, expected dict {'from': 'cte' | config_ref() | config_source(), 'columns': ['col'], 'partitions': <int>, 'async': <bool>}, got " ~ tojson(partition_by)) %}
        {% set partition_by = none %}
    {% endif %}

    {% set transient = config.get('transient', false) %}
    {% set cluster_by = config.get('cluster_by') %}
    {% set enable_schema_evolution = config.get('enable_schema_evolution') %}
    {% set data_retention_time_in_days = config.get('data_retention_time_in_days') %}
    {% set max_data_extension_time_in_days = config.get('max_data_extension_time_in_days') %}
    {% set change_tracking = config.get('change_tracking') %}
    {% set default_ddl_collation = config.get('default_ddl_collation') %}
    {% set copy_grants = config.get('copy_grants') %}
    {% set row_access_policy = config.get('row_access_policy') %}
    {% set aggregation_policy = config.get('aggregation_policy') %}
    {% set join_policy = config.get('join_policy') %}
    {% set tags = config.get('tags') %}
    {% set contacts = config.get('contacts') %}

    {% set metadata = {} %}

    {% if cluster_by is iterable %}
        {% set cluster_by = join_strings(cluster_by) %}
        {% do metadata.update({'cluster_by': cluster_by}) %}
    {% elif cluster_by is not none %}
        {% do exceptions.warn("Invalid `cluster_by`, expected string or list of columns, got " ~ tojson(cluster_by)) %}
        {% set cluster_by = none %}
    {% endif %}

    {% if enable_schema_evolution is boolean %}
        {% do metadata.update({'enable_schema_evolution': (enable_schema_evolution | string)}) %}
    {% elif enable_schema_evolution is not none %}
        {% do exceptions.warn("Invalid `enable_schema_evolution`, expected boolean <true | false>, got " ~ tojson(enable_schema_evolution)) %}
        {% set enable_schema_evolution = none %}
    {% endif %}

    {% if data_retention_time_in_days is integer and data_retention_time_in_days >= 0 %}
        {% do metadata.update({'data_retention_time_in_days': data_retention_time_in_days}) %}
    {% elif data_retention_time_in_days is not none %}
        {% do exceptions.warn("Invalid `data_retention_time_in_days`, expected non-negative integer, got " ~ tojson(data_retention_time_in_days)) %}
        {% set data_retention_time_in_days = none %}
    {% endif %}

    {% if max_data_extension_time_in_days is integer and max_data_extension_time_in_days >= 0 %}
        {% do metadata.update({'max_data_extension_time_in_days': max_data_extension_time_in_days}) %}
    {% elif max_data_extension_time_in_days is not none %}
        {% do exceptions.warn("Invalid `max_data_extension_time_in_days`, expected non-negative integer, got " ~ tojson(max_data_extension_time_in_days)) %}
        {% set max_data_extension_time_in_days = none %}
    {% endif %}

    {% if change_tracking is boolean %}
        {% do metadata.update({'change_tracking': (change_tracking | string)}) %}
    {% elif change_tracking is not none %}
        {% do exceptions.warn("Invalid `change_tracking`, expected boolean <true | false>, got " ~ tojson(change_tracking)) %}
        {% set change_tracking = none %}
    {% endif %}

    {% if default_ddl_collation is string %}
        {% do metadata.update({'default_ddl_collation': default_ddl_collation}) %}
    {% elif default_ddl_collation is not none %}
        {% do exceptions.warn("Invalid `default_ddl_collation`, expected string, got " ~ tojson(default_ddl_collation)) %}
        {% set default_ddl_collation = none %}
    {% endif %}

    {% if row_access_policy is mapping and row_access_policy.get('policy') is iterable and row_access_policy.get('columns') is iterable %}
        {% set row_access_policy = {
            'policy': (compile_config(row_access_policy.get('policy')) | string),
            'columns': join_strings(row_access_policy.get('columns'))
        } %}
        {% do metadata.update({'row_access_policy': row_access_policy}) %}
    {% elif row_access_policy is not none %}
        {% do exceptions.warn("Invalid `row_access_policy`, expected dict {'policy': config_ref('policy'), 'columns': ['col']}, got " ~ tojson(row_access_policy)) %}
        {% set row_access_policy = none %}
    {% endif %}

    {% if has_keys(aggregation_policy, 'policy') and (aggregation_policy.get('entity_keys') is none or aggregation_policy.get('entity_keys') is iterable) %}
        {% set temp = {'policy': (compile_config(aggregation_policy.get('policy')) | string)} %}
        {% if aggregation_policy.get('entity_keys') is not none %}
            {% do temp.update({'entity_keys': join_strings(aggregation_policy.get('entity_keys'))}) %}
        {% endif %}
        {% set aggregation_policy = temp %}
        {% do metadata.update({'aggregation_policy': aggregation_policy}) %}
    {% elif aggregation_policy is not none %}
        {% do exceptions.warn("Invalid `aggregation_policy`, expected dict {'policy': config_ref('policy'), 'entity_keys': ['col']}, got " ~ tojson(aggregation_policy)) %}
        {% set aggregation_policy = none %}
    {% endif %}

    {% if has_keys(join_policy, 'policy') and (aggregation_policy.get('allowed_join_keys') is none or aggregation_policy.get('allowed_join_keys') is iterable) %}
        {% set temp = {'policy': (compile_config(join_policy.get('policy')) | string)} %}
        {% if join_policy.get('allowed_join_keys') is not none %}
            {% do temp.update({'allowed_join_keys': join_strings(join_policy.get('allowed_join_keys'))}) %}
        {% endif %}
        {% set join_policy = temp %}
        {% do metadata.update({'join_policy': join_policy}) %}
    {% elif join_policy is not none %}
        {% do exceptions.warn("Invalid `join_policy`, expected dict {'policy': config_ref('policy'), 'allowed_join_keys': ['col']}, got " ~ tojson(join_policy)) %}
        {% set join_policy = none %}
    {% endif %}

    {% if tags is iterable and tags is not string %}
        {% set temp = {} %}
        {% for tag in tags if tag is mapping and 'tag' in tag and 'value' in tag %}
            {% do temp.update({(compile_config(tag.get('tag')) | string): tag.get('value')}) %}
        {% endfor %}
        {% set tags = temp %}
        {% do metadata.update({'tags': tags}) %}
    {% elif tags is not none %}
        {% do exceptions.warn("Invalid `tags`, expected list [{'tag': config_ref('tag'), 'value': 'value'}, ...], got " ~ tojson(tags)) %}
        {% set tags = none %}
    {% endif %}

    {% if contacts is mapping %}
        {% set temp = {} %}
        {% for k, v in contacts.items() %}
            {% do temp.update({k: compile_config(v) | string}) %}
        {% endfor %}
        {% set contacts = temp %}
        {% do metadata.update({'contacts': contacts}) %}
    {% elif contacts is not none %}
        {% do exceptions.warn("Invalid `contacts`, expected dict {'approver': config_ref('contact'), ...}, got " ~ tojson(contacts)) %}
        {% set contacts = none %}
    {% endif %}

    {% set query | replace('\n        ', '\n') %}
        select
            ifnull(any_value(DDL), 'create or replace') as DDL
        from
            (
                select
                    regexp_substr("comment", 'metadata[:] [{](.|\s)*[}]') as "comment_metadata",
                    try_parse_json(right("comment_metadata", len("comment_metadata") - 10)) as "metadata",
                    case
                        {%- if transient %}
                        when "kind" != 'TRANSIENT' then 'create or replace'
                        {%- elif transient is not none %}
                        when "kind" = 'TRANSIENT' then 'create or replace'
                        {%- endif %}
                        when "metadata" != try_parse_json('{{ escape_ansii(tojson(metadata)) }}') then 'create or replace'
                        else 'create if not exists'
                    end as DDL
                from
                    $1
            )
    {% endset %}

    {% set DDL = drop_relation(this, unless_type='table', query=query)['DDL'] %}

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- BEGIN happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    --------------------------------------------------------------------------------------------------------------------
    -- build model

    {% set temp_relation = make_temp_relation(this) %}
    {% set pfilter = partition_filter() %}

    {% set query | replace('\n        ', '\n') | trim %}
        create or replace temporary {{ tmp_relation_type }} {{ temp_relation }} as
            select * from (
                {{ (sql | indent(12)).replace('                        \n', '\n') }}
            ) where false limit 0
        ->> describe {{ tmp_relation_type }} {{ temp_relation }}
    {% endset %}

    {% set columns = [] %}

    {% for row in run_query(query) %}
        {% do columns.append((row["name"], row["type"])) %}
    {% endfor %}

    {% set create_or_replace | replace('\n        ', '\n') | trim %}
        create or replace {{- ' transient' if transient }} table {{ this }}(
            {%- for column in columns %}
            {{ adapter.quote(column[0]) }} {{ column[1] }} {{- ',' if not loop.last }}
            {%- endfor %}
        )
            {%- if cluster_by is not none and cluster_by != '' %}
            cluster by ({{ cluster_by }})
            {%- endif %}
            {%- if enable_schema_evolution %}
            enable_schema_evolution = true
            {%- elif enable_schema_evolution is not none %}
            enable_schema_evolution = false
            {%- endif %}
            {%- if data_retention_time_in_days is not none %}
            data_retention_time_in_days = {{ data_retention_time_in_days }}
            {%- endif %}
            {%- if max_data_extension_time_in_days is not none %}
            max_data_extension_time_in_days = {{ max_data_extension_time_in_days }}
            {%- endif %}
            {%- if change_tracking %}
            change_tracking = true
            {%- elif change_tracking is not none %}
            change_tracking = false
            {%- endif %}
            {%- if default_ddl_collation is not none %}
            default_ddl_collation = '{{ escape_ansii(default_ddl_collation) }}'
            {%- endif %}
            {%- if copy_grants %}
            copy grants
            {%- endif %}
            comment = 'metadata: {{ escape_ansii(tojson(metadata)) }}'
            {%- if row_access_policy is not none %}
            with row access policy {{ row_access_policy['policy'] }} on ({{ row_access_policy['columns'] }})
            {%- endif %}
            {%- if aggregation_policy is not none %}
            with aggregation policy {{ aggregation_policy['policy'] }}
                {{- ' (' ~ aggregation_policy['entity_keys'] ~ ')' if 'entity_keys' in aggregation_policy }}
            {%- endif %}
            {%- if join_policy is not none %}
            with join policy {{ join_policy['policy'] }}
                {{- ' (' ~ join_policy['allowed_join_keys'] ~ ')' if 'allowed_join_keys' in join_policy }}
            {%- endif %}
            {%- if tags is not none %}
            with tags(
                {%- for k, v in tags | dictsort -%}
                {{ k }} = '{{ escape_ansii(v) }}' {{- ', ' if not loop.last }}
                {%- endfor -%}
            )
            {%- endif %}
            {%- if contacts is not none %}
            with contacts(
                {%- for k, v in contacts | dictsort -%}
                {{ k }} = {{ v }} {{- ', ' if not loop.last }}
                {%- endfor -%}
            )
            {%- endif %}
    {% endset %}

    {% if DDL != 'create or replace' %}
        {% set query | replace('\n            ', '\n') | trim %}
            describe table {{ this }}
            ->> describe {{ tmp_relation_type }} {{ temp_relation }}
            ->> drop {{ tmp_relation_type }} if exists {{ temp_relation }}
            ->> select
                    count(*)
                from
                    (
                        select hash_agg("name", "type") from $2
                        union
                        select hash_agg("name", "type") from $3
                    )
        {% endset %}

        {% if run_query(query)[0][0] == 2 %}
            {% set DDL = 'create or replace' %}
        {% endif %}
    {% endif %}

    {% if has_keys(partition_by, 'partitions') and partition_by.get('columns') | length == 1 %}
        {% set query | replace('\n            ', '\n') | trim %}
            select distinct
                {{ partition_by.get('columns') }}
            from
                {{ partition_by.get('from') }}
            order by
                nvl2({{ partition_by.get('columns') }}, hash({{ partition_by.get('columns') }}), null) nulls first
            limit
                {{ 100 * partition_by.get('partitions') }}
            ->> select
                    ifnull(to_json(array_agg({{ partition_by.get('columns') }}) within group (order by {{ partition_by.get('columns') }})), '[]') as "partitions",
                    count_if({{ partition_by.get('columns') }} is null) as "nulls"
                from
                    $1
        {% endset %}

        {% set row = run_query(remove_select(sql) ~ query)[0] %}
        {% set partitions = fromjson(row[0]) %}
        {% set null_partitions = row[1] %}

        {% if partitions | length < 2 %}
            {% set partition_by = none %}
        {% endif %}
    {% endif %}

    {% if partition_by is none %}
        {% set query | replace('\n            ', '\n') | trim %}
            {% if DDL == 'create or replace' -%}
            drop {{ tmp_relation_type }} if exists {{ temp_relation }} ->>
            {{ create_or_replace | indent(12) }} ->>
            {% endif -%}
            insert overwrite into {{ this }}(
                {%- for column in columns -%}
                {{ adapter.quote(column[0]) }} {{- ', ' if not loop.last }}
                {%- endfor -%}
            )
                {{ (sql | indent(16)).replace('                \n', '\n') }}
        {% endset %}

    {% elif has_keys(partition_by, 'partitions') and partition_by.get('columns') | length == 1 %}
        {% if partitions | length >= partition_by.get('partitions') %}
            {% set size = (partitions | length) // partition_by.get('partitions') %}
            {% set remainder = (partitions | length) % partition_by.get('partitions') %}
            {% set state = {'index': -1} %}

            {% set temp = [] %}

            {% for i in range(partition_by.get('partitions')) %}
                {% if i < remainder %}
                    {% do state.update({'index': state['index'] + size + 1}) %}
                {% else %}
                    {% do state.update({'index': state['index'] + size}) %}
                {% endif %}
                {% if not loop.last %}
                    {% do temp.append(partitions[state['index']]) %}
                {% endif %}
            {% endfor %}

            {% set partitions = temp %}
        {% endif %}

        {% if partition_by.get('async') %}

            {% set query | replace('\n                ', '\n') %}
                begin
                    {%- if DDL == 'create or replace' %}
                    drop {{ tmp_relation_type }} if exists {{ temp_relation }};
                    {{ create_or_replace | indent(20) }};
                    {%- else %}
                    truncate table if exists {{ this }};
                    {%- endif %}

                    {%- set filter_inequality | trim %}
                        {%- if partitions[0] is string %}
                            where {{ partition_by.get('columns') }} < '{{ escape_ansii(partitions[0]) }}'
                        {%- else %}
                            where {{ partition_by.get('columns') }} < {{ partitions[0] }}
                        {%- endif %}
                    {%- endset %}

                    async(
                        insert into {{ this }}(
                            {%- for column in columns -%}
                            {{ adapter.quote(column[0]) }} {{- ', ' if not loop.last }}
                            {%- endfor -%}
                        )
                            {{ (sql.replace(pfilter, filter_inequality) | indent(28)).replace('                            \n', '\n') }}
                    );

                    {%- set filter_inequality | trim %}
                        {%- if partitions[0] is string %}
                            where {{ partition_by.get('columns') }} >= '{{ escape_ansii(partitions[-1]) }}'
                        {%- else %}
                            where {{ partition_by.get('columns') }} >= {{ partitions[-1] }}
                        {%- endif %}
                        {%- if null_partitions > 0 %} or {{ partition_by.get('columns') }} is null {%- endif %}
                    {%- endset %}

                    async(
                        insert into {{ this }}(
                            {%- for column in columns -%}
                            {{ adapter.quote(column[0]) }} {{- ', ' if not loop.last }}
                            {%- endfor -%}
                        )
                            {{ (sql.replace(pfilter, filter_inequality) | indent(32)).replace('                            \n', '\n') }}
                    );

                {%- set indexes = {} %}

                {%- for i in range(partitions | length - 1) %}
                    {%- do indexes.update({local_md5((run_started_at | string) ~ (i | string)): i}) %}
                {%- endfor %}

                {%- for k, i in indexes | dictsort %}
                    {%- set filter_inequality | trim %}
                        {%- if partitions[0] is string %}
                            where {{ partition_by.get('columns') }} >= '{{ escape_ansii(partitions[i]) }}' and {{ partition_by.get('columns') }} < '{{ escape_ansii(partitions[i + 1]) }}'
                        {%- else %}
                            where {{ partition_by.get('columns') }} >= {{ partitions[i] }} and {{ partition_by.get('columns') }} < {{ partitions[i + 1] }}
                        {%- endif %}
                    {%- endset %}

                    async(
                        insert into {{ this }}(
                            {%- for column in columns -%}
                            {{ adapter.quote(column[0]) }} {{- ', ' if not loop.last }}
                            {%- endfor -%}
                        )
                            {{ (sql.replace(pfilter, filter_inequality) | indent(28)).replace('                            \n', '\n') }}
                    );
                {%- endfor %}

                    await all;
                end
            {% endset %}
        {% else %}
            {% set query | replace('\n                ', '\n') %}
                {%- if DDL == 'create or replace' %}
                drop {{ tmp_relation_type }} if exists {{ temp_relation }};
                ->> {{ create_or_replace | indent(20) }}
                {%- else %}
                truncate table if exists {{ this }}
                {%- endif %}

                {%- set filter_inequality | trim %}
                    {%- if partitions[0] is string %}
                        where {{ partition_by.get('columns') }} < '{{ escape_ansii(partitions[0]) }}'
                    {%- else %}
                        where {{ partition_by.get('columns') }} < {{ partitions[0] }}
                    {%- endif %}
                {%- endset %}

                ->> insert into {{ this }}(
                        {%- for column in columns -%}
                        {{ adapter.quote(column[0]) }} {{- ', ' if not loop.last }}
                        {%- endfor -%}
                    )
                        {{ (sql.replace(pfilter, filter_inequality) | indent(24)).replace('                            \n', '\n') }}

                {%- for i in range(partitions | length - 1) %}
                    {%- set filter_inequality | trim %}
                        {%- if partitions[0] is string %}
                            where {{ partition_by.get('columns') }} >= '{{ escape_ansii(partitions[i]) }}' and {{ partition_by.get('columns') }} < '{{ escape_ansii(partitions[i + 1]) }}'
                        {%- else %}
                            where {{ partition_by.get('columns') }} >= {{ partitions[i] }} and {{ partition_by.get('columns') }} < {{ partitions[i + 1] }}
                        {%- endif %}
                    {%- endset %}

                ->> insert into {{ this }}(
                        {%- for column in columns -%}
                        {{ adapter.quote(column[0]) }} {{- ', ' if not loop.last }}
                        {%- endfor -%}
                    )
                        {{ (sql.replace(pfilter, filter_inequality) | indent(24)).replace('                            \n', '\n') }}
                {%- endfor %}

                {%- set filter_inequality | trim %}
                    {%- if partitions[0] is string %}
                        where {{ partition_by.get('columns') }} >= '{{ escape_ansii(partitions[-1]) }}'
                    {%- else %}
                        where {{ partition_by.get('columns') }} >= {{ partitions[-1] }}
                    {%- endif %}
                    {%- if null_partitions > 0 %} or {{ partition_by.get('columns') }} is null {%- endif %}
                {%- endset %}

                ->> insert into {{ this }}(
                        {%- for column in columns -%}
                        {{ adapter.quote(column[0]) }} {{- ', ' if not loop.last }}
                        {%- endfor -%}
                    )
                        {{ (sql.replace(pfilter, filter_inequality) | indent(24)).replace('                        \n', '\n') }}
            {% endset %}
        {% endif %}

    {% elif has_keys(partition_by, 'partitions') %}
        {% set partition_relation = make_temp_relation(temp_relation) %}

        {% set query | replace('\n            ', '\n') %}
            begin
                create or replace temporary table {{ partition_relation }} as
                    {{ (remove_select(sql) | indent(20)).replace('                    \n', '\n') | trim }}

                    select distinct
                        {%- for column in partition_by.get('columns') %}
                        {{ column }} as {{ adapter.quote(local_md5(column | string)) }} {{- ',' if not loop.last }}
                        {%- endfor %}
                    from
                        {{ partition_by.get('from') }}
                    order by
                        {%- for column in partition_by.get('columns') %}
                        {{ loop.index }} {{- ',' if not loop.last }}
                        {%- endfor %};

                {% if DDL == 'create or replace' and tmp_relation_type == 'view' -%}
                drop {{ tmp_relation_type }} if exists {{ temp_relation }};
                {% endif -%}
                create or replace temporary read only table {{ temp_relation }} clone {{ partition_relation }};
                drop table if exists {{ partition_relation }};

                let N int := (select count(*) from {{ temp_relation }});
                let M int := round(N / {{ partition_by.get('partitions') }});

                if (M * {{ partition_by.get('partitions') }} >= N) then
                    M := M - 1;
                endif;

                {%- if DDL == 'create or replace' %}
                {{ create_or_replace | indent(16) }};
                {%- else %}
                truncate table if exists {{ this }};
                {%- endif %}

                for i in 0 to M do
                    {%- set row_filter | trim %}
                    inner join {{ temp_relation }} on metadata$row_position between ({{ partition_by.get('partitions') }} * :i) and ({{ partition_by.get('partitions') }} * (:i + 1) - 1)
                    {%- for column in partition_by.get('columns') %} and {{ column }} is not distinct from {{ adapter.quote(local_md5(column | string)) }}
                    {%- endfor %}
                    {%- endset %}
                    {%- if partition_by.get('async') %}
                    async(
                        insert into {{ temp_relation }}
                            {{ (sql.replace(pfilter, row_filter) | indent(28)).replace('                            \n', '\n') }}
                    );
                    {%- else %}
                    insert into {{ temp_relation }}
                        {{ (sql.replace(pfilter, row_filter) | indent(24)).replace('                        \n', '\n') }};
                    {%- endif %}
                end for;

                drop table if exists {{ temp_relation }};
                {%- if partition_by.get('async') %}
                await all;
                {%- endif %}
            end
        {% endset %}

    {% else %}
        {% set partition_relation = make_temp_relation(temp_relation) %}

        {% set query | replace('\n            ', '\n') %}
            begin
                create or replace temporary table {{ partition_relation }} as
                    {{ (remove_select(sql) | indent(20)).replace('                    \n', '\n') | trim }}

                    select
                        * exclude "hash(sysdate(), *)"
                    from (
                        select
                            *, hash(sysdate(), *) as "hash(sysdate(), *)"
                        from (
                            select distinct
                                {%- for column in partition_by.get('columns') %}
                                {{ column }} as {{ adapter.quote(local_md5(column | string)) }} {{- ',' if not loop.last }}
                                {%- endfor %}
                            from
                                {{ partition_by.get('from') }}
                        )
                    )
                    order by
                        "hash(sysdate(), *)";

                {% if DDL == 'create or replace' and tmp_relation_type == 'view' -%}
                drop {{ tmp_relation_type }} if exists {{ temp_relation }};
                {% endif -%}
                create or replace temporary read only table {{ temp_relation }} clone {{ partition_relation }};
                drop table if exists {{ partition_relation }};

                let N int := (select count(*) from {{ temp_relation }});

                {%- if DDL == 'create or replace' %}
                {{ create_or_replace | indent(16) }};
                {%- else %}
                truncate table if exists {{ this }};
                {%- endif %}

                for i in 0 to N - 1 do
                    {%- set row_filter | trim %}
                    inner join {{ temp_relation }} on metadata$row_position = :i
                    {%- for column in partition_by.get('columns') %} and {{ column }} is not distinct from {{ adapter.quote(local_md5(column | string)) }}
                    {%- endfor %}
                    {%- endset %}
                    {%- if partition_by.get('async') %}
                    async(
                        insert into {{ temp_relation }}
                            {{ (sql.replace(pfilter, row_filter) | indent(28)).replace('                            \n', '\n') }}
                    );
                    {%- else %}
                    insert into {{ temp_relation }}
                        {{ (sql.replace(pfilter, row_filter) | indent(24)).replace('                        \n', '\n') }};
                    {%- endif %}
                end for;

                drop table if exists {{ temp_relation }};
                {%- if partition_by.get('async') %}
                await all;
                {%- endif %}
            end
        {% endset %}
    {% endif %}

    {% if ';' in query %}
        {%- call statement('main') -%}
            execute immediate '{{ escape_ansii(query) }}'
        {%- endcall -%}
    {% else %}
        {%- call statement('main') -%}
            {{- query -}}
        {%- endcall -%}
    {% endif %}

    --------------------------------------------------------------------------------------------------------------------

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {% if config.get('grants') is not none %}
        {% do apply_model_grants(this, config.get('grants'), 'table') %}
    {% endif %}

    {% if config.persist_relation_docs() %}
        {% do persist_model_docs(this, model, 'table', tojson(metadata)) %}
    {% endif %}

    {% do unset_query_tag(original_query_tag) %}

    -- return
    {{ return({'relations': [this]}) }}

{% endmaterialization %}
