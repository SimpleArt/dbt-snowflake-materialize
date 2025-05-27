{% materialization script, adapter='snowflake' %}
    {% set original_query_tag = set_query_tag() %}
    {% set sql_header = config.get('sql_header') %}

    {% set returns = config.get('returns', 'varchar') %}
    {% set language = config.get('language') %}

    {% if language is string %}
        {% set language = {language: []} %}
    {% endif %}

    {% set data_types = [] %}

    {% set target_relation = get_fully_qualified_relation(this) %}

    {% if should_full_refresh() %}
        {% set script_materialization = config.get('script_materialization', 'full refresh') %}
    {% else %}
        {% set script_materialization = config.get('script_materialization') %}
    {% endif %}

    {% if script_materialization in ['table', 'view'] %}
        {% set target_relation = target_relation.incorporate(type=script_materialization) %}
    {% endif %}

    {% if script_materialization is not none %}
        {% do drop_relation_unless(target_relation, script_materialization) %}
    {% endif %}

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- BEGIN happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    --------------------------------------------------------------------------------------------------------------------
    -- build model

    {% set script_block %}
        with run_all as procedure()
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

        call run_all()
    {% endset %}

    {% set uncommented = uncomment_sql(sql.strip().lower()) %}

    {% call statement('main') %}
        {%- if uncommented.startswith('declare') or uncommented.startswith('begin') %}
        {{ sql_run_safe(script_block) }}
        {%- else %}
        {{ sql }}
        {%- endif %}
    {% endcall %}

    --------------------------------------------------------------------------------------------------------------------
    -- build model
    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {% do unset_query_tag(original_query_tag) %}

    -- return
    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
