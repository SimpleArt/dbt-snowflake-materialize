{% materialization materialized_view, adapter='snowflake', supported_languages=['sql'] %}
    {% set original_query_tag = set_query_tag() %}
    {% set sql_header = config.get('sql_header') %}

    {% set secure = config.get('secure', false) %}
    {% set copy_grants = config.get('copy_grants', false) %}
    {% set cluster_by = config.get('cluster_by') %}

    {% set target_relation = get_fully_qualified_relation(this).incorporate(type='view') %}

    {% set sql_hash = local_md5(sql) %}

    {% set DDL = drop_relation_unless(target_relation, 'materialized view', ['Query Hash: ' ~ sql_hash])['DDL'] %}

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- BEGIN happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    --------------------------------------------------------------------------------------------------------------------
    -- build model
    {% if DDL == 'create if not exists' %}

        {% call statement('main') %}
            {{ sql_header if sql_header is not none }}

        {%- if cluster_by is none %}
            alter materialized view {{ target_relation }}
                drop clustering keys
        ->>
        {%- endif %}
            create {{- " secure" if secure }} materialized view if not exists {{ target_relation }}
                {%- if cluster_by is not none %}
                cluster by ({{ cluster_by | join(', ') }})
                {%- endif %}
            as
                /* Query Hash: {{ sql_hash }} */ {{ sql }}
        {%- if secure %}
        ->> alter materialized view {{ target_relation }} set secure
        {%- elif secure is not none %}
        ->> alter materialized view {{ target_relation }} unset secure
        {%- endif %}
        {%- if cluster_by is not none %}
        ->>
            alter materialized view {{ target_relation }}
                cluster by ({{ cluster_by | join(', ') }})
        {%- endif %}
        {% endcall %}

    {% else %}

        {% call statement('main') %}
            {{ sql_header if sql_header is not none }}

            create or replace {{- " secure" if secure }} materialized view {{ target_relation }}
                {%- if copy_grants %}
                copy grants
                {%- endif %}
                {%- if cluster_by is not none %}
                cluster by ({{ cluster_by | join(', ') }})
                {%- endif %}
            as
                /* Query Hash: {{ sql_hash }} */ {{ sql }}
        {% endcall %}

    {% endif %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {% if config.get('grants') is not none %}
        {% do custom_apply_grants(target_relation, config.get('grants'), 'materialized view') %}
    {% endif %}

    {% if config.persist_relation_docs() %}
        {% do custom_persist_docs(target_relation, model, 'materialized view') %}
    {% endif %}

    {% do unset_query_tag(original_query_tag) %}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
