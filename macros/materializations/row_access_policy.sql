{% materialization row_access_policy, adapter='snowflake', supported_languages=['sql'] %}
    {% set original_query_tag = set_query_tag() %}

    {% set parameters = config.get('parameters', '') %}

    {% set parameters_hash = local_md5(parameters|string) %}
    {% set body_hash = local_md5(sql) %}

    {% set target_relation = get_fully_qualified_relation(this) %}

    {% set DDL = drop_relation_unless(
        target_relation,
        'row access policy',
        metadata=['Parameter Hash: ' ~ parameters_hash],
        alter_if=['Body Hash: ' ~ body_hash]
    )['DDL'] %}

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- BEGIN happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    --------------------------------------------------------------------------------------------------------------------
    -- build model
    {% if DDL == 'create if not exists' %}
        {% call statement('main') %}
            create row access policy if not exists {{ target_relation }} as ({{ parameters }})
                returns boolean
                -> {{ sql }}
        {% endcall %}

    {% elif DDL == 'alter if exists' %}
        {% call statement('main') %}
            alter row access policy {{ target_relation }} set body
                -> {{ sql }}
        ->>
            alter row access policy {{ target_relation }} set comment = 'Parameter Hash: {{ parameters_hash }}\nBody Hash: {{ body_hash }}'
        {% endcall %}

    {% else %}
        {% call statement('main') %}
            create or replace row access policy {{ target_relation }} as ({{ parameters }})
                returns boolean
                -> {{ sql }}
        ->>
            alter row access policy {{ target_relation }} set comment = 'Parameter Hash: {{ parameters_hash }}\nBody Hash: {{ body_hash }}'
        {% endcall %}

    {% endif %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {% if config.get('grants') is not none %}
        {% do custom_apply_grants(target_relation, config.get('grants'), 'row access policy') %}
    {% endif %}

    {% if config.persist_relation_docs() %}
        {% do custom_persist_docs(target_relation, model, 'row access policy', 'Parameter Hash: ' ~ parameters_hash ~ '\\nBody Hash: ' ~ body_hash) %}
    {% endif %}

    {% do unset_query_tag(original_query_tag) %}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
