{% materialization script, adapter='snowflake' %}
    {% set original_query_tag = set_query_tag() %}
    {% set sql_header = config.get('sql_header') %}

    {% set anonymous_procedure = config.get('anonymous_procedure') %}
    {% set script_materialization = config.get('script_materialization') %}
    {% set drop_if = config.get('drop_if') %}

    {% if anonymous_procedure is mapping %}
        {% set returns = anonymous_procedure.get('returns', 'varchar') %}
        {% set procedure_config = anonymous_procedure.get('procedure_config') %}

        {% if procedure_config is iterable and procedure_config is not string %}
            {% set temp = [] %}
            {% for line in procedure_config %}
                {% do temp.append(compile_config(line)) %}
            {% endfor %}
            {% set procedure_config = temp %}
        {% elif procedure_config is not none %}
            {% set procedure_config = [procedure_config] %}
        {% endif %}
    {% endif %}

    {% set target_relation = get_fully_qualified_relation(this) %}

    {% if script_materialization in ['table', 'view'] %}
        {% set target_relation = target_relation.incorporate(type=script_materialization) %}
    {% endif %}

    {% if script_materialization is not none %}
        {% if drop_if is none %}
            {% do drop_relation(target_relation, unless_type=script_materialization) %}
        {% else %}
            {% set query | replace('\n            ', '\n') %}
                {{ drop_if }}
            ->> select iff(count(*) = 0, 'create if not exists', 'create or replace') from $1
            {% endset %}

            {% set drop_result = drop_relation(target_relation, unless_type=script_materialization, query=query) %}

            {% if drop_result['DDL'] == 'create or replace' and drop_result.get('type') == script_materialization %}
                {% do drop_relation(target_relation, unless_type=none) %}
            {% endif %}
        {% endif %}
    {% endif %}

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- BEGIN happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    --------------------------------------------------------------------------------------------------------------------
    -- build model

    {% if anonymous_procedure is mapping %}
        {% set query | replace('\n            ', '\n') %}
            with anonymous_procedure as procedure()
                returns {{ returns }}
                {%- if procedure_config is not none %}
                {%- for line in procedure_config %}
                {{ line }}
                {%- endfor %}
                {%- endif %}
            as '{{ escape_ansii(sql) }}'

            call anonymous_procedure()
        {% endset %}
    {% else %}
        {% set query = sql %}
    {% endif %}

    {% if ';' in query %}
        {%- call statement('main') -%}
            execute immediate '{{ escape_ansii(query) }}'
        {%- endcall -%}
    {% else %}
        {%- call statement('main') -%}
            {{ query }}
        {%- endcall -%}
    {% endif %}

    --------------------------------------------------------------------------------------------------------------------
    -- build model
    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {% do unset_query_tag(original_query_tag) %}

    -- return
    {% if script_materialization in ['table', 'view'] %}
        {{ return({'relations': [this.incorporate(type=script_materialization)]}) }}
    {% else %}
        {{ return({'relations': [this]}) }}
    {% endif %}

{% endmaterialization %}
