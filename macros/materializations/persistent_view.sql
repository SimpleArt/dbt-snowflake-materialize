{% materialization persistent_view, adapter='snowflake', supported_languages=['sql'] %}
    {% set original_query_tag = set_query_tag() %}
    {% set sql_header = config.get('sql_header') %}

    {% set secure = config.get('secure', false) %}
    {% set recursive = config.get('recursive', false) %}
    {% set change_tracking = config.get('change_tracking', false) %}
    {% set copy_grants = config.get('copy_grants', false) %}

    {% if recursive %}
        {% set change_tracking = false %}
    {% endif %}

    {% set target_relation = get_fully_qualified_relation(this).incorporate(type='view') %}

    {% set sql_hash = local_md5(sql) %}

    {% set DDL = drop_relation_unless(target_relation, 'view', ['Recursive: ' ~ recursive, 'Query Hash: ' ~ sql_hash]) %}

    {% if should_full_refresh() %}
        {% set DDL = 'create or replace' %}
    {% endif %}

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- BEGIN happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    --------------------------------------------------------------------------------------------------------------------
    -- build model
    {% if DDL == 'create if not exists' %}

        {% if not change_tracking %}
            {% call statement('set_change_tracking') %}
                alter view if exists {{ target_relation }} set
                    change_tracking = false
            {% endcall %}
        {% endif %}

        {% call statement('main') %}
            {{ sql_header if sql_header is not none }}

            create {{- " secure" if secure }} {{- " recursive" if recursive }} view if not exists {{ target_relation }}
                change_tracking = {{ change_tracking }}
            as
                /* Recursive: {{ recursive }} */
                /* Query Hash: {{ sql_hash }} */
                {{ sql }}
        {% endcall %}

        {% if secure %}
            {% call statement('set_secure') %}
                alter view {{ target_relation }} set secure
            {% endcall %}
        {% else %}
            {% call statement('unset_secure') %}
                alter view {{ target_relation }} unset secure
            {% endcall %}
        {% endif %}

        {% if change_tracking %}
            {% call statement('set_change_tracking') %}
                alter view if exists {{ target_relation }} set
                    change_tracking = true
            {% endcall %}
        {% endif %}

    {% else %}

        {% call statement('main') %}
            {{ sql_header if sql_header is not none }}

            create or replace {{- " secure" if secure }} {{- " recursive" if recursive }} view {{ target_relation }}
                change_tracking = {{ change_tracking }}
                {%- if copy_grants %}
                copy grants
                {%- endif %}
            as
                /* Recursive: {{ recursive }} */
                /* Query Hash: {{ sql_hash }} */
                {{ sql }}
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
        {% do custom_persist_docs(target_relation, model) %}
    {% endif %}

    {% do unset_query_tag(original_query_tag) %}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
