{% materialization clone, adapter='snowflake', supported_languages=['sql'] %}
    {% set original_query_tag = set_query_tag() %}
    {% set sql_header = config.get('sql_header') %}

    {% set transient = config.get('transient', true) %}
    {% set copy_grants = config.get('copy_grants', false) %}

    {% set target_relation = this.incorporate(type='table') %}
    {% set should_revoke = drop_relation(target_relation) and copy_grants %}

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- BEGIN happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    --------------------------------------------------------------------------------------------------------------------
    -- build model
    {% call statement('main') %}
        {{ sql_header if sql_header is not none }}

        create or replace {{- " transient" if transient }} table {{ target_relation }}
            {{ sql }}
            {% if copy_grants %}
            copy grants
            {% endif %}
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
