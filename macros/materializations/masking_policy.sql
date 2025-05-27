{% materialization masking_policy, adapter='snowflake', supported_languages=['sql'] %}
    {% set original_query_tag = set_query_tag() %}

    {% set parameters = config.get('parameters', '') %}
    {% set returns = config.get('returns') %}
    {% set exempt_other_policies = config.get('exempt_other_policies', false) %}

    {% set parameters_hash = local_md5(parameters|string) %}
    {% set body_hash = local_md5(sql) %}

    {% set target_relation = get_fully_qualified_relation(this) %}

    {% set DDL = drop_relation_unless(
        target_relation,
        'row access policy',
        metadata=['Parameters Hash: ' ~ parameters_hash, 'Returns: ' ~ returns, 'Exempt Other Policies: ' ~ exempt_other_policies],
        alter_if=['Body Hash: ' ~ body_hash]
    )['DDL'] %}

    {% set comment -%}
    Parameters Hash: {{ parameters_hash }}\nReturns: {{ returns }}\nBody Hash: {{ body_hash }}\nExempt Other Policies: {{ exempt_other_policies }}
    {%- endset %}

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- BEGIN happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    --------------------------------------------------------------------------------------------------------------------
    -- build model
    {% if DDL == 'create if not exists' %}
        {% call statement('main') %}
            create masking policy if not exists {{ target_relation }} as ({{ parameters }})
                returns {{ returns }}
                -> {{ sql }}
                {%- if exempt_other_policies %}
                exempt_other_policies = true
                {%- endif %}
        {% endcall %}

    {% elif DDL == 'alter if exists' %}
        {% call statement('main') %}
            alter masking policy {{ target_relation }} set body -> {{ sql }} 
            ->> alter row access policy {{ target_relation }} set comment = '{{ comment }}'
        {% endcall %}

    {% else %}

        {% call statement('main') %}
            create or replace masking policy {{ target_relation }} as ({{ parameters }})
                returns {{ returns }}
                -> {{ sql }}
                comment = '{{ comment }}'
                {%- if exempt_other_policies %}
                exempt_other_policies = true
                {%- endif %}
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
        {% do custom_persist_docs(target_relation, model, 'row access policy', comment) %}
    {% endif %}

    {% do unset_query_tag(original_query_tag) %}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
