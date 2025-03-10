{% materialization stream, adapter='snowflake', supported_languages=['sql'] %}
    {% set original_query_tag = set_query_tag() %}
    {% set sql_header = config.get('sql_header') %}

    {% set copy_grants = config.get('copy_grants', false) %}
    {% set append_only = config.get('append_only') %}
    {% set show_initial_rows = config.get('show_initial_rows') %}

    {% if append_only is none %}
        {% set append_only = false %}
    {% endif %}

    {% if show_initial_rows is none %}
        {% set show_initial_rows = true %}
    {% endif %}

    {% set target_relation = get_fully_qualified_relation(this) %}

    {% set query = ' ' ~ (sql.lower().split() | join(' ')).replace('*/', '*/ ') %}

    {% if ' on table ' in query %}
        {% set source_type = 'table' %}
    {% elif ' on view ' in query %}
        {% set source_type = 'view' %}
    {% elif ' on external table ' in query %}
        {% set source_type = 'external table' %}
    {% elif ' on stage ' in query %}
        {% set source_type = 'stage' %}
    {% else %}
        {% set source_type = none %}
    {% endif %}

    {% set sql_hash = local_md5(
        local_md5(append_only|string)
        ~ local_md5(sql)
    ) %}

    {% set DDL = drop_relation_unless(target_relation, 'stream', ['Query Hash: ' ~ sql_hash]) %}

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- BEGIN happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    --------------------------------------------------------------------------------------------------------------------
    -- build model
    {% if DDL == 'create or replace' %}

        {% call statement('main') %}
            {{ sql_header if sql_header is not none }}

            create or replace stream {{ target_relation }}
                {%- if copy_grants %}
                copy grants
                {%- endif %}
                {{ sql.strip() }}
                {%- if append_only %}
                {%- if source_type == 'external table' %}
                insert_only = true
                {%- elif source_type in ['table', 'view'] %}
                append_only = true
                {%- endif %}
                {%- endif %}
                {%- if show_initial_rows and source_type in ['table', 'view', 'external table'] %}
                show_initial_rows = true
                {%- endif %}
                comment = 'Query Hash: {{ sql_hash }}'
        {% endcall %}

    {% else %}

        {% call statement('main') %}
            {{ sql_header if sql_header is not none }}

            create stream if not exists {{ target_relation }}
                {{ sql.strip() }}
                {%- if append_only %}
                {%- if source_type == 'external table' %}
                insert_only = true
                {%- elif source_type in ['table', 'view'] %}
                append_only = true
                {%- endif %}
                {%- endif %}
                {%- if show_initial_rows and source_type in ['table', 'view', 'external table'] %}
                show_initial_rows = true
                {%- endif %}
                comment = 'Query Hash: {{ sql_hash }}'
        {% endcall %}

    {% endif %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {% if config.get('grants') is not none %}
        {% do custom_apply_grants(target_relation, config.get('grants'), 'stream') %}
    {% endif %}

    {% if config.persist_relation_docs() %}
        {% do custom_persist_docs(target_relation, model, 'stream', 'Query Hash: ' ~ sql_hash) %}
    {% endif %}

    {% do unset_query_tag(original_query_tag) %}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
