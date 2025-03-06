{% materialization sql_script, adapter='snowflake' %}
    --------------------------------------------------------------------------------------------------------------------

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    --------------------------------------------------------------------------------------------------------------------

    {% set full_query %}
        with run_all as procedure()
            returns table()
        as '{{ sql.replace("'", "\\'") }}'

        call run_all()
    {% endset %}

    -- build model
    {% call statement('main') %}
        {{ sql_run_safe(full_query) }}
    {% endcall %}

   --------------------------------------------------------------------------------------------------------------------
    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    -- return
    {{ return({'relations': [this]}) }}

{% endmaterialization %}
