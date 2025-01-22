{% materialization persistent_view, adapter='snowflake', supported_languages=['sql'] %}
    {% set original_query_tag = set_query_tag() %}
    {% set grant_config = config.get('grants') %}
    {% set sql_header = config.get('sql_header') %}

    {% set secure = config.get('secure', false) %}
    {% set recursive = config.get('recursive', false) %}
    {% set copy_grants = config.get('copy_grants', false) %}

    {% set target_relation = this.incorporate(type='view') %}

    {% set sql_hash = local_md5(
        local_md5(secure|string)
        ~ local_md5(recursive|string)
        ~ local_md5(sql)
    ) %}

    {% set should_revoke = drop_relation(target_relation) %}

    {% if should_revoke and not should_drop_relation(target_relation, sql_hash, sql) %}

        {% set full_query %}
            create {{- " secure" if secure }} {{- " recursive" if recursive }} view if not exists {{ target_relation }}
                {% if config.persist_relation_docs() %}
                {{- create_with_comments(model, sql) }}
                {% endif %}
            as
                /* Query Hash: {{ sql_hash }} */ {{ sql }}
        {% endset %}

    {% else %}
        {% if not copy_grants %}
            {% set should_revoke = false %}
        {% endif %}

        {% set full_query %}
            create or replace {{- " secure" if secure }} {{- " recursive" if recursive }} view {{ target_relation }}
                {%- if copy_grants %}
                copy grants
                {%- endif %}
                {%- if config.persist_relation_docs() %}
                {{- create_with_comments(model, sql) }}
                {%- endif %}
            as
                /* Query Hash: {{ sql_hash }} */ {{ sql }}
        {% endset %}

    {% endif %}

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- BEGIN happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    --------------------------------------------------------------------------------------------------------------------
    -- build model
    {% call statement('main') %}
        {{ sql_header if sql_header is not none }}
        {{ full_query }}
    {% endcall %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {% if config.get('grants') is not none %}
        {% do post_apply_grants(target_relation, config.get('grants'), should_revoke) %}
    {% endif %}

    {% if config.persist_relation_docs() %}
        {% do post_persist_docs(target_relation, model) %}
    {% endif %}

    {% do unset_query_tag(original_query_tag) %}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
