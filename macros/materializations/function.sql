{% materialization function, adapter='snowflake' %}
    {% set original_query_tag = set_query_tag() %}
    {% set sql_header = config.get('sql_header') %}

    {% set secure = config.get('secure') %}
    {% set aggregate = config.get('aggregate', false) %}
    {% set parameters = config.get('parameters', '') %}
    {% set copy_grants = config.get('copy_grants', false) %}
    {% set returns = config.get('returns') %}
    {% set function_config = config.get('function_config') %}
    {% set overload_version = config.get('overload_version') %}

    {% if parameters is not string %}
        {% if parameters is iterable %}
            {% set parameters = parameters | join(', ') %}
        {% else %}
            {% set parameters = '' %}
        {% endif %}
    {% endif %}

    {% if function_config is iterable and function_config is not string %}
        {% set temp = [] %}
        {% for line in function_config %}
            {% do temp.append(compile_config(line)) %}
        {% endfor %}
        {% set function_config = temp %}
    {% elif function_config is not none %}
        {% set function_config = [function_config] %}
    {% endif %}

    {% set model_metadata = {
        'parameters': local_md5(parameters|string),
        'returns': returns,
        'function_config': local_md5(function_config|string),
        'body': local_md5(sql)
    } %}

    {% set target_relation = get_fully_qualified_relation(this) %}
    {% set temp_relation = get_fully_qualified_relation(make_temp_relation(target_relation)).incorporate(type='table') %}

    {% set DDL_query | replace('\n        ', '\n') -%}
        {{ show_relation(target_relation, 'user function') }}
        ->> create or replace temporary table {{ temp_relation }} as
                select
                    *,
                    try_parse_json(
                        regexp_substr("description", 'metadata[:] [{](.|\s)*[}]')
                    ) as "model_metadata",
                    regexp_replace(right("arguments", len("arguments") - len("name")), '[)] RETURN(.|\s)*', ')') as "arguments_no_return",
                    'alter function if exists {{ escape_ansii(target_relation) }}' || "arguments_no_return" || ' set comment = {{ escape_ansii("'metadata: " ~ tojson(model_metadata) ~ "'") }}' as "alter_query",
                    'drop function if exists {{ escape_ansii(target_relation) }}' || "arguments_no_return" as "drop_query"
                from
                    $1
        ->> select
                decode(
                    0,
                    count(*), 'create or replace',
                    {%- if secure is not none %}
                    count_if("is_secure" = '{{ "Y" if secure else "N" }}') as 'alter if exists',
                    {%- endif %}
                    'create if not exists'
                ) as "DDL",
                any_value("arguments_no_return") as "arguments_no_return"
            from
                {{ temp_relation }}
            where
                "is_builtin" = 'N'
                and "is_aggregate" = '{{ "Y" if aggregate else "N" }}'
                {%- for k, v in model_metadata | dictsort %}
                and "model_metadata":{{ k }} = '{{ escape_ansii(v) }}'
                {%- endfor %}
    {% endset %}

    {% set drop_result = drop_relation(target_relation, unless_type='function', query=DDL_query) %}
    {% set DDL = drop_result['DDL'] %}

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- BEGIN happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    --------------------------------------------------------------------------------------------------------------------
    -- build model

    {%- if DDL == 'create or replace' %}

        {% set drop_query | trim %}
            select "drop_query" from {{ temp_relation }}
        {% endset %}

        {% set main_query | replace('\n            ', '\n') %}
            begin
                {% if sql_header is not none -%}

                {{ sql_header }}

                {% endif -%}

                let res resultset := (
                    create or replace {{- " secure" if secure }} {{- " aggregate" if aggregate }} function {{ target_relation }}({{ parameters }})
                        {{- " copy grants" if copy_grants }}
                        returns {{ returns }}
                        {%- if function_config is not none %}
                        {%- for line in function_config %}
                        {{ line }}
                        {%- endfor %}
                        {%- endif %}
                    as '{{ escape_ansii(sql) }}'
                );

                return table(res);

            exception when other then
                {%- for row in run_query(drop_query) %}
                {{ row.get('drop_query') }};
                {%- endfor %}

                let retry_res resultset := (
                    create or replace {{- " secure" if secure }} {{- " aggregate" if aggregate }} function {{ target_relation }}({{ parameters }})
                        {{- " copy grants" if copy_grants }}
                        returns {{ returns }}
                        {%- if function_config is not none %}
                        {%- for line in function_config %}
                        {{ line }}
                        {%- endfor %}
                        {%- endif %}
                    as '{{ escape_ansii(sql) }}'
                );

                return table(retry_res);
            end
        {% endset %}

    {% elif DDL == 'alter if exists' %}
        {% set main_query | replace('\n            ', '\n') %}
            {%- for row in drop_result.get('rows', []) %}
            alter function if exists {{ target_relation }}({{ row.get('arguments_no_return') }})
                {{- 'set' if secure else 'unset' }} secure
            {%- endfor %}
        {% endset %}

    {% else %}
        {% set main_query | replace('\n            ', '\n') %}
            create {{- " secure" if secure }} {{- " aggregate" if aggregate }} function if not exists {{ target_relation }}({{ parameters }})
                returns {{ returns }}
                {%- if function_config is not none %}
                {%- for line in function_config %}
                {{ line }}
                {%- endfor %}
                {%- endif %}
            as '{{ escape_ansii(sql) }}'
        {% endset %}

    {% endif %}

    {% call statement('main') %}
        {%- if ';' in main_query -%}
        execute immediate '{{ escape_ansii(main_query) }}'
        {%- else -%}
        {{ main_query }}
        {%- endif -%}
    {% endcall %}

    {%- if DDL == 'create or replace' %}

        {% set query | replace('\n        ', '\n') %}
            {{ show_relation(target_relation, 'user procedure') }}
        ->> select
                "created_on",
                regexp_replace(right("arguments", len("arguments") - len("name")), '[)] RETURN(.|\s)*', ')') as "arguments_no_return",
                'alter procedure if exists {{ escape_ansii(target_relation) }}' || "arguments_no_return" || ' set comment = {{ escape_ansii("'metadata: " ~ tojson(model_metadata) ~ "'") }}' as "alter_query"
            from
                $1
        ->> select
                "arguments_no_return",
                "alter_query"
            from
                (
                    select
                        "created_on",
                        "arguments_no_return",
                        "alter_query"
                    from
                        $1

                    minus

                    select
                        "created_on",
                        "arguments_no_return",
                        "alter_query"
                    from
                        {{ temp_relation }}
                )
            qualify
                count(*) over () = 1
        {% endset %}

        {% set rows = run_query(query) %}

        {% set post_query | replace('\n            ', '\n') %}
                {% for row in rows -%}
                {{ row.get('alter_query') }}
            ->> {% endfor -%}
                drop table if exists {{ temp_relation }}
        {% endset %}

        {% do run_query(post_query) %}

        {% set arguments = (rows | first | default({})).get('arguments_no_return', '()') %}

    {% else %}

        {% set post_query | replace('\n        ', '\n') %}
            select
                "arguments_no_return"
            from
                {{ temp_relation }}
            where
                "is_builtin" = 'N'
                and "is_aggregate" = '{{ "Y" if aggregate else "N" }}'
                {%- if secure is not none %}
                and "is_secure" = '{{ "Y" if secure else "N" }}'
                {%- endif %}
                {%- for k, v in model_metadata | dictsort %}
                and "model_metadata":{{ k }} = '{{ escape_ansii(v) }}'
                {%- endfor %}
        ->> drop table if exists {{ temp_relation }}
        ->> select * from $2
        {%- endset -%}

        {% set arguments = (run_query(post_query) | first | default({})).get('arguments_no_return', '()') %}

    {% endif %}

    --------------------------------------------------------------------------------------------------------------------
    -- build model
    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {% set target_relation_with_arguments | trim %}
        {{ target_relation }}{{ arguments }}
    {% endset %}

    {% if config.get('grants') is not none %}
        {% do apply_model_grants(target_relation_with_arguments, config.get('grants'), 'function') %}
    {% endif %}

    {% if config.persist_relation_docs() %}
        {% do persist_model_docs(target_relation_with_arguments, model, 'function', tojson(model_metadata)) %}
    {% endif %}

    {% if overload_version is boolean and overload_version %}
        {% set overload_version = model.get('latest_version') | string %}
    {% elif overload_version is not none %}
        {% set overload_version = overload_version | string %}
    {% endif %}

    {% if overload_version is not none and model.get('version') is not none and overload_version != (model.get('version') | string) %}
        {% set current_version = model.get('version') | string %}
        {% set overload_identifier = target_relation.identifier %}

        {% if target_relation.identifier.endswith('"') %}
            {% set overload_identifier = overload_identifier[:-1] %}
        {% endif %}

        {% if target_relation.identifier.endswith('_V' ~ current_version) %}
            {% set overload_identifier = overload_identifier[:-(current_version | length)] %}
        {% elif target_relation.identifier.endswith('_v' ~ current_version) %}
            {% set overload_identifier = overload_identifier[:-(current_version | length)] %}
        {% else %}
            {% set overload_identifier = overload_identifier ~ '_v' %}
        {% endif %}

        {% if overload_version == '0' %}
            {% set overload_identifier = overload_identifier[:-2] %}
        {% else %}
            {% set overload_identifier = overload_identifier ~ overload_version %}
        {% endif %}

        {% if target_relation.identifier.endswith('"') %}
            {% set overload_identifier = overload_identifier ~ '"' %}
        {% endif %}

        {% set overload_relation = target_relation.incorporate(
            path={'identifier': overload_identifier}
        ) %}

        {% set drop_result = drop_relation(overload_relation, unless_type='function', query=DDL_query) %}
        {% set DDL = drop_result['DDL'] %}

        {% set drop_query | trim %}
            select "drop_query" from {{ temp_relation }}
        {% endset %}

        {% set overload_relation_with_arguments | trim %}
            {{ overload_relation }}{{ arguments }}
        {% endset %}

        {%- if DDL == 'create or replace' %}

            {% set main_query | replace('\n            ', '\n') %}
                begin
                    {% if sql_header is not none -%}

                    {{ sql_header }}

                    {% endif -%}

                    let res resultset := (
                        create or replace {{- " secure" if secure }} {{- " aggregate" if aggregate }} function {{ overload_relation }}({{ parameters }})
                            {{- " copy grants" if copy_grants }}
                            returns {{ returns }}
                            {%- if function_config is not none %}
                            {%- for line in function_config %}
                            {{ line }}
                            {%- endfor %}
                            {%- endif %}
                        as '{{ escape_ansii(sql) }}'
                    );

                    alter function {{ overload_relation_with_arguments }} set
                        comment = '{{ escape_ansii(tojson(model_metadata)) }}';

                    drop table if exists {{ temp_relation }};

                    return table(res);
                exception when other then
                    {%- for row in run_query(drop_query) %}
                    {{ row.get('drop_query') }};
                    {%- endfor %}

                    let retry_res resultset := (
                        create or replace {{- " secure" if secure }} {{- " aggregate" if aggregate }} function {{ overload_relation }}({{ parameters }})
                            {{- " copy grants" if copy_grants }}
                            returns {{ returns }}
                            {%- if function_config is not none %}
                            {%- for line in function_config %}
                            {{ line }}
                            {%- endfor %}
                            {%- endif %}
                        as '{{ escape_ansii(sql) }}'
                    );

                    alter function {{ overload_relation_with_arguments }} set
                        comment = '{{ escape_ansii(tojson(model_metadata)) }}';

                    drop table if exists {{ temp_relation }};

                    return table(retry_res);
                end
            {% endset %}

            {% call statement('overloaded') %}
                execute immediate '{{- escape_ansii(main_query) -}}'
            {% endcall %}

        {% elif DDL == 'alter if exists' %}

            {% call statement('overloaded') %}
                alter function {{ overload_relation_with_arguments }}
                    {{- 'set' if secure else 'unset' }} secure
            {% endcall %}

        {%- endif %}

        {% if config.get('grants') is not none %}
            {% do apply_model_grants(overload_relation_with_arguments, config.get('grants'), 'function') %}
        {% endif %}

        {% if config.persist_relation_docs() %}
            {% do persist_model_docs(overload_relation_with_arguments, model, 'function', tojson(model_metadata)) %}
        {% endif %}

    {% endif %}

    {% do unset_query_tag(original_query_tag) %}

    -- return
    {{ return({'relations': [this]}) }}

{% endmaterialization %}
