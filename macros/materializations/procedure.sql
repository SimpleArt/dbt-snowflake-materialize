{% materialization procedure, adapter='snowflake' %}
    {% set original_query_tag = set_query_tag() %}
    {% set sql_header = config.get('sql_header') %}

    {% set secure = config.get('secure') %}
    {% set parameters = config.get('parameters', '') %}
    {% set copy_grants = config.get('copy_grants', false) %}
    {% set returns = config.get('returns', 'varchar') %}
    {% set language = config.get('language') %}
    {% set overload_version = config.get('overload_version') %}

    {% set data_types = [] %}

    {% for parameter in parameters.upper().split(',') %}
        {% do data_types.append(parameter.split(' DEFAULT ')[0].split()[-1]) %}
    {% endfor %}

    {% set arguments = '(' ~ data_types | join(', ') ~ ')' %}

    {% set target_relation = get_fully_qualified_relation(this) %}

    {% set sql_hash = local_md5(
        local_md5(parameters|string)
        ~ local_md5(returns|string)
        ~ local_md5(language|string)
        ~ local_md5(sql)
    ) %}

    {% set DDL = drop_relation_unless(target_relation, 'procedure', ['Query Hash: ' ~ sql_hash]) %}

    {% if should_full_refresh() %}
        {% set DDL = 'create or replace' %}
    {% endif %}

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- BEGIN happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    --------------------------------------------------------------------------------------------------------------------
    -- build model

    {% set create_or_replace %}
        {{ sql_header if sql_header is not none }}

        create or replace {{- " secure" if secure }} procedure {{ target_relation }}({{ parameters }})
            {{- " copy grants" if copy_grants }}
            returns {{ returns }}
            {%- if language is not none %}
            {%- for language_name, language_configs in language.items() %}
            language {{ language_name }}
            {%- for language_config in language_configs %}
            {{ language_config }}
            {%- endfor %}
            {%- endfor %}
            {%- endif %}
        as $${{ sql }}$$
    {% endset %}

    {% if DDL == 'create if not exists' %}
        {% call statement('main') %}
            select 'already exists' as status
        {% endcall %}

        {% if secure %}
            {% call statement('set_secure') %}
                alter procedure {{ target_relation }}{{ arguments }} set secure
            {% endcall %}
        {% elif secure is not none %}
            {% call statement('unset_secure') %}
                alter procedure {{ target_relation }}{{ arguments }} unset secure
            {% endcall %}
        {% endif %}

    {% else %}
        {% set status = run_query(sql_try_except(create_or_replace))[0]['STATUS'] %}

        {% if status == 'success' %}
            {% call statement('main') %}
                select 'success' as status
            {% endcall %}

        {% else %}
            {% do drop_relation_unless(target_relation, 'table', ['Query Hash: ' ~ sql_hash]) %}

            {% call statement('main') %}
                {{- sql_run_safe(create_or_replace) -}}
            {% endcall %}

        {% endif %}

        {% call statement('save_hash') %}
            alter procedure {{ target_relation }}{{ arguments }} set comment = $$Query Hash: {{ sql_hash }}$$
        {% endcall %}

    {% endif %}

    --------------------------------------------------------------------------------------------------------------------
    -- build model
    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {% if config.get('grants') is not none %}
        {% do custom_apply_grants(target_relation, config.get('grants'), 'procedure', arguments) %}
    {% endif %}

    {% if config.persist_relation_docs() %}
        {% do custom_persist_docs(target_relation, model, 'procedure', '\nQuery Hash: ' ~ sql_hash, arguments) %}
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

        {% set DDL = drop_relation_unless(overload_relation, 'procedure', ['Query Hash: ' ~ sql_hash]) %}

        {% set create_or_replace %}
            {{ sql_header if sql_header is not none }}

            create or replace {{- " secure" if secure }} procedure {{ overload_relation }}({{ parameters }})
                {{- " copy grants" if copy_grants }}
                returns {{ returns }}
                {%- if language is not none %}
                {%- for language_name, language_configs in language.items() %}
                language {{ language_name }}
                {%- for language_config in language_configs %}
                {{ language_config }}
                {%- endfor %}
                {%- endfor %}
                {%- endif %}
            as $${{ sql }}$$
        {% endset %}

        {% if DDL == 'create if not exists' %}
            {% if secure %}
                {% call statement('set_secure') %}
                    alter procedure {{ overload_relation }}{{ arguments }} set secure
                {% endcall %}
            {% elif secure is not none %}
                {% call statement('unset_secure') %}
                    alter procedure {{ overload_relation }}{{ arguments }} unset secure
                {% endcall %}
            {% endif %}

        {% else %}
            {% set status = run_query(sql_try_except(create_or_replace))[0]['STATUS'] %}

            {% if status != 'success' %}
                {% do drop_relation_unless(overload_relation, 'table', ['Query Hash: ' ~ sql_hash]) %}
                {% do run_query(create_or_replace) %}
            {% endif %}

            {% call statement('save_hash') %}
                alter procedure {{ overload_relation }}{{ arguments }} set comment = $$Query Hash: {{ sql_hash }}$$
            {% endcall %}

        {% endif %}

        {% if config.get('grants') is not none %}
            {% do custom_apply_grants(overload_relation, config.get('grants'), 'procedure', arguments) %}
        {% endif %}

        {% if config.persist_relation_docs() %}
            {% do custom_persist_docs(overload_relation, model, 'procedure', '\nQuery Hash: ' ~ sql_hash, arguments) %}
        {% endif %}

    {% endif %}

    {% do unset_query_tag(original_query_tag) %}

    -- return
    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
