{% materialization function, adapter='snowflake' %}
    {% set original_query_tag = set_query_tag() %}
    {% set sql_header = config.get('sql_header') %}

    {% set secure = config.get('secure') %}
    {% set aggregate = config.get('aggregate', false) %}
    {% set parameters = config.get('parameters', '') %}
    {% set copy_grants = config.get('copy_grants', false) %}
    {% set returns = config.get('returns') %}
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

    {% set DDL = drop_relation_unless(target_relation, 'function', ['Aggregate: ' ~ aggregate, 'Query Hash: ' ~ sql_hash])['DDL'] %}

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- BEGIN happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    --------------------------------------------------------------------------------------------------------------------
    -- build model

    {% set create_or_replace %}
        {{ sql_header if sql_header is not none }}

        create or replace {{- " secure" if secure }} {{- " aggregate" if aggregate }} function {{ target_relation }}({{ parameters }})
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
        as '{{ quote_sql(sql) }}'
    {% endset %}

    {% if DDL == 'create if not exists' %}
        {% call statement('main') %}
        {%- if secure %}
            alter function {{ target_relation }}{{ arguments }} set secure
        {%- elif secure is not none %}
            alter function {{ target_relation }}{{ arguments }} unset secure
        {%- else %}
            select 'already exists' as status
        {%- endif %}
        {% endcall %}

    {% else %}
        {% set status = run_query(sql_try_except(create_or_replace))[0]['STATUS'] %}

        {% if status == 'success' %}
            {% set statement_name = 'main' %}

        {% else %}
            {% set statement_name = 'alter_comment' %}

            {% do drop_relation_unless(target_relation, 'table', ['Aggregate: ' ~ aggregate, 'Query Hash: ' ~ sql_hash]) %}

            {% call statement('main') %}
                {{- sql_run_safe(create_or_replace) -}}
            {% endcall %}

        {% endif %}

        {% call statement(statement_name) %}
            alter function {{ target_relation }}{{ arguments }} set comment = $$Aggregate: {{ aggregate }}\nQuery Hash: {{ sql_hash }}$$
        {% endcall %}

    {% endif %}

    --------------------------------------------------------------------------------------------------------------------
    -- build model
    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {% if config.get('grants') is not none %}
        {% do custom_apply_grants(target_relation, config.get('grants'), 'function', arguments) %}
    {% endif %}

    {% if config.persist_relation_docs() %}
        {% do custom_persist_docs(target_relation, model, 'function', 'Aggregate: ' ~ aggregate ~ '\\nQuery Hash: ' ~ sql_hash, arguments) %}
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

        {% set DDL = drop_relation_unless(overload_relation, 'function', ['Aggregate: ' ~ aggregate, 'Query Hash: ' ~ sql_hash])['DDL'] %}

        {% set create_or_replace %}
            {{ sql_header if sql_header is not none }}

            create or replace {{- " secure" if secure }} {{- " aggregate" if aggregate }} function {{ overload_relation }}({{ parameters }})
                returns {{ returns }}
                {%- if language is not none %}
                {%- for language_name, language_configs in language.items() %}
                language {{ language_name }}
                {%- for language_config in language_configs %}
                {{ language_config }}
                {%- endfor %}
                {%- endfor %}
                {%- endif %}
            as '{{ quote_sql(sql) }}'
        {% endset %}

        {% if DDL == 'create if not exists' %}
            {% if secure %}
                {% call statement('set_secure') %}
                    alter function {{ overload_relation }}{{ arguments }} set secure
                {% endcall %}
            {% elif secure is not none %}
                {% call statement('unset_secure') %}
                    alter function {{ overload_relation }}{{ arguments }} unset secure
                {% endcall %}
            {% endif %}

        {% else %}
            {% set status = run_query(sql_try_except(create_or_replace))[0]['STATUS'] %}

            {% if status != 'success' %}
                {% do drop_relation_unless(overload_relation, 'table', ['Aggregate: ' ~ aggregate, 'Query Hash: ' ~ sql_hash]) %}
                {% do run_query(create_or_replace) %}
            {% endif %}

            {% call statement('save_hash') %}
                alter function {{ overload_relation }}{{ arguments }} set comment = $$Aggregate: {{ aggregate }}\nQuery Hash: {{ sql_hash }}$$
            {% endcall %}

        {% endif %}

        {% if config.get('grants') is not none %}
            {% do custom_apply_grants(overload_relation, config.get('grants'), 'function', arguments) %}
        {% endif %}

        {% if config.persist_relation_docs() %}
            {% do custom_persist_docs(overload_relation, model, 'function', 'Aggregate: ' ~ aggregate ~ '\\nQuery Hash: ' ~ sql_hash, arguments) %}
        {% endif %}

    {% endif %}

    {% do unset_query_tag(original_query_tag) %}

    -- return
    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
