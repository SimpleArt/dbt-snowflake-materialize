{% materialization task, adapter='snowflake', supported_languages=['sql'] %}
    {% set original_query_tag = set_query_tag() %}
    {% set sql_header = config.get('sql_header') %}

    {% set configurations = [
        'tags',
        'contacts',
        'warehouse',
        'user_task_managed_inital_warehouse_size',
        'schedule',
        'task_config',
        'allow_overlapping_execution',
        'session_parameters',
        'user_task_timeout_ms',
        'suspend_task_after_num_failures',
        'error_integration',
        'success_integration',
        'log_level',
        'comment',
        'finalize',
        'task_auto_retry_attempts',
        'user_task_minimum_trigger_interval_in_seconds',
        'target_completion_interval',
        'serverless_task_min_statement_size',
        'serverless_task_max_statement_size',
        'after',
        'when'
    ] %}

    {% set create_or_alterable = [
        'warehouse',
        'user_task_managed_inital_warehouse_size',
        'schedule',
        'task_config',
        'allow_overlapping_execution',
        'user_task_timeout_ms',
        'session_parameters',
        'suspend_task_after_num_failures',
        'error_integration',
        'success_integration',
        'comment',
        'finalize',
        'task_auto_retry_attempts',
        'after',
        'when'
    ] %}

    {% set alter_settable = [
        'warehouse',
        'user_task_managed_inital_warehouse_size',
        'schedule',
        'task_config',
        'allow_overlapping_execution',
        'user_task_timeout_ms',
        'suspend_task_after_num_failures',
        'error_integration',
        'success_integration',
        'log_level',
        'comment',
        'session_parameters',
        'task_auto_retry_attempts',
        'user_task_minimum_trigger_interval_in_seconds',
        'target_completion_interval',
        'serverless_task_min_statement_size',
        'serverless_task_max_statement_size',
        'contacts'
    ] %}

    {% set alter_unsettable = [
        'warehouse',
        'schedule',
        'task_config',
        'allow_overlapping_execution',
        'user_task_timeout_ms',
        'suspend_task_after_num_failures',
        'log_level',
        'target_completion_interval',
        'serverless_task_min_statement_size',
        'serverless_task_max_statement_size'
    ] %}

    {% set set_configs = {
        'after': none,
        'when': none,
    } %}

    {% for name in alter_unsettable %}
        {% do set_configs.update({name: none}) %}
    {% endfor %}

    {% for configuration in configurations if configuration != 'comment' and configuration in config %}
        {% do set_configs.update({configuration: config.get(configuration)}) %}
    {% endfor %}

    {% for configuration in ['tags', 'contacts', 'error_integration', 'success_integration', 'finalize', 'after']
        if set_configs.get(configuration) is string
    %}
        {% do set_configs.update({configuration: parse_jinja(set_configs.get(configuration))['code']}) %}
    {% endfor %}

    {% if set_configs.get('tags') is mapping %}
        {% set tags = {} %}

        {% for k, v in set_configs.get('tags').items() %}
            {% if k is string %}
                {% do tags.update({parse_jinja(k)['code']: v}) %}
            {% else %}
                {% do tags.update({k: v}) %}
            {% endif %}
        {% endfor %}

        {% do set_configs.update({'tags': tags}) %}
    {% endif %}

    {% if set_configs.get('contacts') is mapping %}
        {% set contacts = {} %}

        {% for k, v in set_configs.get('contacts').items() %}
            {% if v is string %}
                {% do contacts.update({k: parse_jinja(v)['code']}) %}
            {% else %}
                {% do contacts.update({k: v}) %}
            {% endif %}
        {% endfor %}

        {% do set_configs.update({'contacts': contacts}) %}
    {% endif %}

    {% if set_configs.get('after') is iterable and set_configs.get('after') is not string %}
        {% set after_tasks = [] %}
        {% for task in set_configs.get('after') %}
            {% if task is string %}
                {% do after_tasks.append(parse_jinja(task)['code']) %}
            {% else %}
                {% do after_tasks.append(task) %}
            {% endif %}
        {% endfor %}
        {% do set_configs.update({'after': after_tasks}) %}
    {% endif %}

    {% set alter_if = ['Task Hash: ' ~ local_md5(sql | string)] %}

    {% for configuration_name, configuration_value in set_configs.items() %}
        {% do alter_if.append(configuration_name.replace('task_config', 'config').replace('_', ' ').title().replace(' Ms', ' MS') ~ ': ' ~ (configuration_value | string)) %}
    {% endfor %}

    {% if alter_if == [] %}
        {% set alter_if = none %}
    {% else %}
        {% do set_configs.update({'comment': alter_if | join(', ')}) %}
    {% endif %}

    {% set drop_result = drop_relation_unless(this, 'task', alter_if=alter_if) %}

    {% set DDL = drop_result['DDL'] %}

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=false) }}

    -- BEGIN happens here:
    {{ run_hooks(pre_hooks, inside_transaction=true) }}

    --------------------------------------------------------------------------------------------------------------------
    -- build model

    {% set uncommented = uncomment_sql(sql.lower().strip()) %}

    {% if uncommented.startswith('declare') or uncommented.startswith('begin') %}
        {% set task_body -%}
            execute immediate '{{ quote_sql(sql) }}'
        {%- endset %}
    {% else %}
        {% set task_body = sql %}
    {% endif %}

    {% if DDL == 'alter if exists' %}
        {% set altered = [] %}

        {% for name in configurations + ['task hash'] %}
        {% for altered_result in drop_result['alter_if']
            if altered_result.startswith(name.replace('task_config', 'config').replace('_', ' ').title().replace(' Ms', ' MS'))
        %}
        {% do altered.append(name) %}
        {% endfor %}
        {% endfor %}

        {% call statement('main') -%}
            {%- set unset_configurations = [] %}
            {%- for name in array_intersection(altered, alter_unsettable)
                if set_configs.get(name) is none
            %}
            {%- do unset_configurations.append(name) %}
            {%- endfor %}
            {%- if unset_configurations != [] %}
            alter task if exists {{ this }} unset
                {%- for name in unset_configurations %}
                {{ name }}
                {%- endfor %}
        ->>
            {%- set altered = array_difference(altered, unset_configurations) %}
            {%- endif %}
            {%- set altered_configurations = [
                'session_parameters',
                'finalize',
                'after',
                'when',
                'task hash'
            ] %}
            {%- if array_intersection(altered_configurations, altered) != [] %}
            create or alter task {{ this }}
                {%- if set_configs.get('warehouse') is not none %}
                warehouse = {{ set_configs.get('warehouse') }}
                {%- endif %}
                {%- for name in [
                    'user_task_managed_inital_warehouse_size',
                    'schedule'
                ] if set_configs.get(name) is not none %}
                {{ name }} = '{{ set_configs.get(name) }}'
                {%- endfor %}
                {%- if set_configs.get('task_config') is not none %}
                config = '{{ quote_sql(set_configs.get('task_config')) }}'
                {%- endif %}
                {%- for name in [
                    'allow_overlapping_execution',
                    'user_task_timeout_ms'
                ] if set_configs.get(name) is not none %}
                {{ name }} = {{ set_configs.get(name) }}
                {%- endfor %}
                {%- set session_parameters = set_configs.get('session_parameters') %}
                {%- if session_parameters is mapping %}
                {%- for k, v in session_parameters.items() %}
                {%- if v is string %}
                {{ k }} = '{{ quote_sql(v) }}'
                {%- else %}
                {{ k }} = {{ v }}
                {%- endif %}
                {%- endfor %}
                {%- elif session_parameters is iterable and session_parameters is not string %}
                {{ session_parameters | join(', ') }}
                {%- elif session_parameters is not none %}
                {{ session_parameters }}
                {%- endif %}
                {%- for name in [
                    'suspend_task_after_num_failures',
                    'error_integration',
                    'success_integration',
                    'finalize',
                    'task_auto_retry_attempts'
                ] if set_configs.get(name) is not none %}
                {{ name }} = {{ set_configs.get(name) }}
                {%- endfor %}
                {%- set after_config = set_configs.get('after') %}
                {%- if after_config is iterable and after_config is not string %}
                after {{ after_config | join(', ') }}
                {%- elif after_config is not none %}
                after {{ after_config }}
                {%- endif %}
                {%- if set_configs.get('when') is not none %}
                when {{ set_configs.get('when') }}
                {%- endif %}
            as
                {{ task_body }}
        ->>
            {%- set altered_configurations = [
                'warehouse',
                'user_task_managed_initial_warehouse_size',
                'schedule',
                'task_config',
                'allow_overlapping_execution',
                'user_task_timeout_ms',
                'session_parameters',
                'suspend_task_after_num_failures',
                'error_integration',
                'success_integration',
                'finalize',
                'task_auto_retry_attempts',
                'after',
                'when',
                'task_hash'
            ] %}
            {%- set altered = array_difference(altered, altered_configurations) %}
            {%- for name in altered_configurations %}
            {%- do set_configs.update({name: none}) %}
            {%- endfor %}
            {%- endif %}
            {%- set tags = set_configs.get('tags') %}
            {%- if 'tags' in altered and tags is not none %}
            alter task if exists {{ this }} set tag
                {%- if tags is mapping %}
                {%- for k, v in tags.items() %}
                {{ k }} = '{{ quote_sql(v) }}' {{- ',' if not loop.last }}
                {%- endfor %}
                {%- elif tags is iterable and tags is not string %}
                {{ tags | join(', ') }}
                {%- else %}
                {{ tags }}
                {%- endif %}
        ->>
            {%- set altered = array_difference(altered, ['tags']) %}
            {%- endif %}
            {%- set set_configurations = [
                'warehouse',
                'user_task_managed_initial_warehouse_size',
                'schedule',
                'task_config',
                'allow_overlapping_execution',
                'user_task_timeout_ms',
                'suspend_task_after_num_failures',
                'error_integration',
                'success_integration',
                'task_auto_retry_attempts',
                'user_task_minimum_trigger_interval_in_seconds',
                'target_completion_interval',
                'serverless_task_min_statement_size',
                'serverless_task_max_statement_size',
                'contacts'
            ] %}
            alter task if exists {{ this }} set
                {%- if set_configs.get('warehouse') is not none %}
                warehouse = {{ set_configs.get('warehouse') }}
                {%- endif %}
                {%- for name in [
                    'user_task_managed_inital_warehouse_size',
                    'schedule'
                ] if set_configs.get(name) is not none %}
                {{ name }} = '{{ set_configs.get(name) }}'
                {%- endfor %}
                {%- if set_configs.get('task_config') is not none %}
                config = '{{ quote_sql(set_configs.get('task_config')) }}'
                {%- endif %}
                {%- for name in [
                    'allow_overlapping_execution',
                    'user_task_timeout_ms'
                ] if set_configs.get(name) is not none %}
                {{ name }} = {{ set_configs.get(name) }}
                {%- endfor %}
                {%- for name in [
                    'suspend_task_after_num_failures',
                    'error_integration',
                    'success_integration'
                ] if set_configs.get(name) is not none %}
                {{ name }} = {{ set_configs.get(name) }}
                {%- endfor %}
                {%- if set_configs.get('log_level') is not none %}
                log_level = '{{ set_configs.get('log_level') }}'
                {%- endif %}
                comment = '{{ alter_if | join('\\n') }}'
                {%- for name in [
                    'task_auto_retry_attempts',
                    'user_task_minimum_trigger_interval_in_seconds'
                ] if set_configs.get(name) is not none %}
                {{ name }} = {{ set_configs.get(name) }}
                {%- endfor %}
                {%- for name in [
                    'target_completion_interval',
                    'serverless_task_min_statement_size',
                    'serverless_task_max_statement_size'
                ] if set_configs.get(name) is not none %}
                {{ name }} = '{{ set_configs.get(name) }}'
                {%- endfor %}
                {%- set contacts = set_configs.get('contacts') %}
                {%- if contacts is not none %}
                contact(
                {%- if contacts is mapping %}
                {%- for k, v in contacts.items() %}
                {{- k }} = {{ v }} {{- ', ' if not loop.last }}
                {%- endfor %}
                {%- elif contacts is iterable and contacts is not string %}
                {{- contacts | join(', ') }}
                {%- else %}
                {{- contacts }}
                {%- endif -%}
                )
                {%- endif %}
        {%- endcall %}

    {% else %}
        {% call statement('main') %}
            {{ sql_header if sql_header is not none }}

            create {{- ' or replace' if DDL == 'create or replace' }} task {{- ' if not exists' if DDL == 'create if not exists' }} {{ this }}
                {%- set tags = set_configs.get('tags') %}
                {%- if tags is not none %}
                with tag(
                {%- if tags is mapping %}
                {%- for k, v in tags.items() %}
                {{- k }} = '{{ quote_sql(v) }}' {{- ', ' if not loop.last }}
                {%- endfor %}
                {%- elif tags is iterable and tags is not string %}
                {{- tags | join(', ') }}
                {%- else %}
                {{- tags }}
                {%- endif -%}
                )
                {%- endif %}
                {%- set contacts = set_configs.get('contacts') %}
                {%- if contacts is not none %}
                contact(
                {%- if contacts is mapping %}
                {%- for k, v in contacts.items() %}
                {{- k }} = {{ v }} {{- ', ' if not loop.last }}
                {%- endfor %}
                {%- elif contacts is iterable and contacts is not string %}
                {{- contacts | join(', ') }}
                {%- else %}
                {{- contacts }}
                {%- endif -%}
                )
                {%- endif %}
                {%- if set_configs.get('warehouse') is not none %}
                warehouse = {{ set_configs.get('warehouse') }}
                {%- endif %}
                {%- for name in [
                    'user_task_managed_inital_warehouse_size',
                    'schedule'
                ] if set_configs.get(name) is not none %}
                {{ name }} = '{{ set_configs.get(name) }}'
                {%- endfor %}
                {%- if set_configs.get('task_config') is not none %}
                config = '{{ quote_sql(set_configs.get('task_config')) }}'
                {%- endif %}
                {%- if set_configs.get('allow_overlapping_execution') is not none %}
                allow_overlapping_execution = {{ set_configs.get('allow_overlapping_execution') }}
                {%- endif %}
                {%- set session_parameters = set_configs.get('session_parameters') %}
                {%- if session_parameters is mapping %}
                {%- for k, v in session_parameters.items() %}
                {%- if v is string %}
                {{ k }} = '{{ quote_sql(v) }}'
                {%- else %}
                {{ k }} = {{ v }}
                {%- endif %}
                {%- endfor %}
                {%- elif session_parameters is iterable and session_parameters is not string %}
                {{ session_parameters | join(', ') }}
                {%- elif session_parameters is not none %}
                {{ session_parameters }}
                {%- endif %}
                {%- for name in [
                    'user_task_timeout_ms',
                    'suspend_task_after_num_failures',
                    'error_integration',
                    'success_integration'
                ] if set_configs.get(name) is not none %}
                {{ name }} = {{ set_configs.get(name) }}
                {%- endfor %}
                {%- if set_configs.get('log_level') is not none %}
                log_level = '{{ set_configs.get('log_level') }}'
                {%- endif %}
                comment = '{{ alter_if | join('\\n') }}'
                {%- for name in [
                    'finalize',
                    'task_auto_retry_attempts',
                    'user_task_minimum_trigger_interval_in_seconds'
                ] if set_configs.get(name) is not none %}
                {{ name }} = {{ set_configs.get(name) }}
                {%- endfor %}
                {%- for name in [
                    'target_completion_interval',
                    'serverless_task_min_statement_size',
                    'serverless_task_max_statement_size'
                ] if set_configs.get(name) is not none %}
                {{ name }} = '{{ set_configs.get(name) }}'
                {%- endfor %}
                {%- set after_config = set_configs.get('after') %}
                {%- if after_config is iterable and after_config is not string %}
                after {{ after_config | join(', ') }}
                {%- elif after_config is not none %}
                after {{ after_config }}
                {%- endif %}
                {%- if set_configs.get('when') is not none %}
                when {{ set_configs.get('when') }}
                {%- endif %}
            as
                {{ task_body }}
        {% endcall %}
    {% endif %}

    {{ run_hooks(post_hooks, inside_transaction=true) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}
    {{ run_hooks(post_hooks, inside_transaction=false) }}

    {% if config.get('grants') is not none %}
        {% do custom_apply_grants(target_relation, config.get('grants')) %}
    {% endif %}

    {% if config.persist_relation_docs() %}
        {% do custom_persist_docs(target_relation, model, 'task', (alter_if | join('\\n'))) %}
    {% endif %}

    {% do unset_query_tag(original_query_tag) %}

    {{ return({'relations': [this]}) }}
{% endmaterialization %}
