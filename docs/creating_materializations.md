# How to Create Materializations

Want to contribute? Or maybe you want to make your own custom materialization specific to your needs. Whatever the case, this page will take you step-by-step through the process of creating a materialization, starting with the bare minimum followed by more advanced materialization features you'll want to be aware of.

## The Bare Minimum: How do I get Started?

Create a SQL file under `macros/materializations/` for your materialization. Create a `materialization` inside of your file like this:

```sql
{% materialization materialized_view, adapter='snowflake', supported_languages=['sql'] %}
    ...
{% endmaterialization %}
```

This materialization needs to be filled in with the code you want to execute against your database. DBT provides 3 key Jinja variables to help you here:

```sql
{% materialization materialized_view, adapter='snowflake', supported_languages=['sql'] %}
    {# Configurations set using config(...) in DBT models. #}
    {% set optional_config = config.get('optional_config', 'default if any') %}
    {% set required_config = config.require('required_config') %}

    {# Run hooks for extra SQL queries submitted before the main code. #}
    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {# The main query that needs to be run for this model. #}
    {% call statement('main') %}
        create or replace materialized view {{ this }} as
            {{ sql }}
    {% endcall %}

    {# Run hooks for extra SQL queries submitted after the main code. #}
    {{ run_hooks(post_hooks, inside_transaction=True) }}
    {{ adapter.commit() }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {# Required return value for the DBT materialization. #}
    {{ return({'relations': [this]}) }}
{% endmaterialization %}
```

1. `config` is the interface used for passing in custom parameters for your materialization. These parameters may either be optional or required.
2. `this` is the name of the model you are creating. Use this when writing your `create` statement.
3. `sql` is the compiled Jinja code for the model being created. Usually, this should be a select query.

##### Additional Notes:
1. You must wrap the main query you plan to execute using `{% call statement('main') %}`.
2. You must wrap the main query inside of the `{{ run_hooks(...) }}` and `{{ adapter.commit() }}`.

Let's look at how to use this materialization in a model:

```sql
{{ config(
    materialized='materialized_view',
    optional_config='optional value',
    required_config='required value'
) }}

-- My select query.
select * from {{ source('source_name', 'source_table') }}
```

Now that you know how to create a simple DBT materialization, let's look at more advanced cases.

## Changing Model Materializations

You've created your new custom materialization, but you've quickly realized a huge issue. Your model already exists in Snowflake as a `view`, but you want to change it into a `table`. Snowflake throws an error when you try to run your `create or replace table` query if it's something other than a `table`, so what should you do?

This is where our package's `drop_relation` macro comes in hand. You can use it to check if your model already exists and drop it unless it is the desired type. Let's add it to our example from before:

```sql
{% materialization materialized_view, adapter='snowflake', supported_languages=['sql'] %}
    {# Configurations set using config(...) in DBT models. #}
    {% set optional_config = config.get('optional_config', 'default if any') %}
    {% set required_config = config.require('required_config') %}

    {# Drop this relation, unless it already exists and is a materialized view. #}
    {% do drop_relation(this, unless_type='materialized view') %}

    {# Run hooks for extra SQL queries submitted before the main code. #}
    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    ...
{% endmaterialization %}
```

This way, while you're testing the performance of specific models using different materializations, you won't need to worry about manually dropping objects in Snowflake and can allow DBT to handle all of the maintenance.

## Temporary Objects

It is not uncommon for a custom materialization to need to create temporary objects. If you only need to create a few temporary objects, then use the `make_temp_relation(...)` macro as follows:

```sql
{% materialization materialized_view, adapter='snowflake', supported_languages=['sql'] %}
    ...

    {# Get DBT generated names for temporary objects. #}
    {% tmp_relation_1 = make_temp_relation(this) %}
    {% tmp_relation_2 = make_temp_relation(tmp_relation_1) %}

    {# Run hooks for extra SQL queries submitted before the main code. #}
    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {# Run queries using the temporary relations. #}
    {% call statement('create_tmp_relation_1') %}
        create or replace temporary view {{ tmp_relation_1 }} as
            {{ sql }}
    {% endcall %}

    {% call statement('create_tmp_relation_2') %}
        create or replace temporary table {{ tmp_relation_2 }} as
            select count(*) as row_count from {{ tmp_relation_1 }}
    {% endcall %}

    {# The main query that needs to be run for this model. #}
    ...

    {# Cleanup the temporary relations. #}
    {% call statement('drop_tmp_relation_1') %}
        drop view if exists {{ tmp_relation_1 }}
    {% endcall %}

    {% call statement('drop_tmp_relation_2') %}
        drop table if exists {{ tmp_relation_2 }}
    {% endcall %}

    {# Run hooks for extra SQL queries submitted after the main code. #}
    {{ run_hooks(post_hooks, inside_transaction=True) }}
    {{ adapter.commit() }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}

    ...
{% endmaterialization %}
```

In rare cases, you may need to dynamically create objects. It's possible to do so as follows:

```sql
{% materialization materialized_view, adapter='snowflake', supported_languages=['sql'] %}
    ...

    {# Run hooks for extra SQL queries submitted before the main code. #}
    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {# Run queries using the temporary relations. #}
    {% for tenant in config.require('tenants') %}
        {# Create a relation for each tenant. #}
        {% set tenant_relation = this.incorporate(path={'identifier': this.identifier ~ '_' ~ tenant}) %}

        {% call statement('create_tenant_relation') %}
            create or replace temporary table {{ tenant_relation }} as
                select * from ({{ sql }}) where tenant = '{{ tenant }}'
        {% endcall %}
    {% endfor %}

    {# The main query that needs to be run for this model. #}
    ...

    {# Cleanup the temporary relations. #}
    {% for tenant in config.require('tenants') %}
        {# Create a relation for each tenant. #}
        {% set tenant_relation = this.incorporate(path={'identifier': this.identifier ~ '_' ~ tenant}) %}

        {% call statement('create_tenant_relation') %}
            drop table if exists {{ tenant_relation }}
        {% endcall %}
    {% endfor %}

    {# Run hooks for extra SQL queries submitted after the main code. #}
    {{ run_hooks(post_hooks, inside_transaction=True) }}
    {{ adapter.commit() }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}

    ...
{% endmaterialization %}
```

## Handling Model Metadata

You may find it useful to store and track metadata about your models. What's the schedule for my task? Is my function memoizable? Sometimes Snowflake stores and returns this metadata easily using the `show` command. Other times, it does not provide this data in a straightforward way, and it may be better to store this metadata ourselves.

Let's look at our original example. There are 2 configurations: an `optional_config` and a `required_config`. We want to save these configurations to our model so that we can run queries when these configurations change. Let's save these configurations after our main query is ran by commenting our model with a JSON object.

```sql
{% materialization materialized_view, adapter='snowflake', supported_languages=['sql'] %}
    {# Configurations set using config(...) in DBT models. #}
    {% set optional_config = config.get('optional_config', 'default if any') %}
    {% set required_config = config.require('required_config') %}

    {# Run hooks for extra SQL queries submitted before the main code. #}
    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {# The main query that needs to be run for this model. #}
    {% call statement('main') %}
        create or replace materialized view {{ this }} as
            {{ sql }}
    {% endcall %}

    {# Save metadata to our model. #}
    {% set metadata = {'optional_config': optional_config, 'required_config': required_config} %}

    {% call statement('save_metadata') %}
        alter materialized view {{ this }} set comment = 'metadata: {{ tojson(metadata) }}'
    {% endcall %}

    {# Run hooks for extra SQL queries submitted after the main code. #}
    {{ run_hooks(post_hooks, inside_transaction=True) }}
    {{ adapter.commit() }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {# Required return value for the DBT materialization. #}
    {{ return({'relations': [this]}) }}
{% endmaterialization %}
```

Now we want to extract this metadata the next time our model runs. We can extract this metadata using `show materialized views like ...`, but writing this ourselves is prone to error, plus we need to post-process the result to extra the JSON we want. Let's look at how to do this using the `drop_relation` macro, which you should already be using.

```sql
{% materialization materialized_view, adapter='snowflake', supported_languages=['sql'] %}
    {# Configurations set using config(...) in DBT models. #}
    {% set optional_config = config.get('optional_config', 'default if any') %}
    {% set required_config = config.require('required_config') %}

    {# Query for what metadata we want to parse out of our "show materialized views" query. #}
    {% set query %}
        select
            regexp_substr("description", 'metadata[:] [{](.|\s)*[}]') as "description_metadata",
            to_json(try_parse_json(right("description_metadata", len("description_metadata") - 10))) as "metadata"
        from
            $1
    {% endset %}

    {# Run "show materialized views" to check if it needs to be dropped. #}
    {% set result = drop_relation(this, unless_type='materialized view', query=query) %}

    {# Collect the metadata from the rows returned by our query. #}
    {% set old_metadata = none %}
    {% set rows = result.get('rows') %}

    {% if rows is not none and (rows | length) > 0 %}
        {% set old_metadata = rows[0].get('metadata') %}
    {% endif %}

    {% if old_metadata is string %}
        {% set old_metadata = fromjson(old_metadata) %}
    {% endif %}

    ...
{% endmaterialization %}
```

Let's go over this:

1. Check what `show materialized views` returns.
2. Write a select query from `$1` to parse the `show materialized views` query.
3. Return results using `to_json`.
4. Run the query using `result = drop_relation(this, unless_type='materialized view', query=query)`.
5. Get the rows returned by our query using `rows = result.get('rows')`.
6. Parse results using `fromjson`.

## Different Types of DDL

There's 3 main types of DDL you may encounter if you do not wish to run a `create or replace` every time, which can become troublesome for materializations such as materialized views. Let's look at the recommended pattern for the 3 types of DDL to consider:

1. `create or replace`: The object should be dropped if it exists, and then created.
2. `create if not exists`: The object should only be created if it doesn't already exist.
3. `create or alter`: The object should be created if it doesn't already exist or altered to match expected configurations.

Determining the type of DDL needed can be done similarly to getting metadata from a model. As part of your query, include a column for the DDL you want to return.

```sql
{% materialization materialized_view, adapter='snowflake', supported_languages=['sql'] %}
    {# Configurations set using config(...) in DBT models. #}
    {% set optional_config = config.get('optional_config', 'default if any') %}
    {% set required_config = config.require('required_config') %}

    {# Define the metadata to compare against. #}
    {% set metadata = {'optional_config': optional_config, 'required_config': required_config} %}

    {# Query for what metadata we want to parse out of our "show materialized views" query. #}
    {% set query %}
        select
            regexp_substr("description", 'metadata[:] [{](.|\s)*[}]') as "description_metadata",
            try_parse_json(right("description_metadata", len("description_metadata") - 10)) as "parsed_json",
            to_json("parsed_json") as "metadata",
            case
                when "parsed_json" = try_parse_json('{{ tojson(metadata) }}') then 'create if not exists'
                else 'create or alter'
            end as DDL
        from
            $1
    {% endset %}

    {# Run "show materialized views" to check if it needs to be dropped. #}
    {% set result = drop_relation(this, unless_type='materialized view', query=query) %}

    {# Get the DDL returned by the query. #}
    {% set DDL = result.get('DDL', 'create or replace') %}

    ...

    {% if DDL == 'create or replace' %}
        {% call statement('main') %}
            create or replace materialized view {{ this }} as
                {{ sql }}
        {% endcall %}

    {% elif DDL == 'create if not exists' %}
        {% call statement('main') %}
            create materialized view if not exists {{ this }} as
                {{ sql }}
        {% endcall %}

    {% else %}
        {% call statement('main') %}
            alter materialized view {{ this }} ...
        {% endcall %}

    {% endif %}

    ...

{% endmaterialization %}
```

Let's recap:

1. Check what `show materialized views` returns.
2. Write a select query from `$1` to parse the `show materialized views` query.
3. Return results in a DDL column.
4. Run the query using `result = drop_relation(this, unless_type='materialized view', query=query)`.
5. Get the DDL using `DDL = result.get('DDL', 'create or replace')`.
6. Write the different cases for our `call statement('main')` depending on the DDL that should be used.

## Submitting Queries with Semicolons

Snowflake does not allow (by default) for multiple queries to be submitted per API call. DBT works around this issue by splitting queries by semicolons, which works for what DBT is built for by default, but does not work for extended use cases like stored procedures. We can work around this using the `escape_ansii` macro.

If you need to include semicolons in a part of a query, such as the definition of a stored procedure, escape just that part of the query:

```sql
{% call statement('main') %}
    create or replace procedure {{ this }}
        returns table()
    as '{{ escape_ansii(sql) }}'
{% endcall %}
```

If you need to include semicolons for the overall query, then wrap your query with `execute immediate`:

```sql
{% call statement('main') %}
    execute immediate '{{ escape_ansii(sql) }}'
{% endcall %}
```

## Submitting Many Queries

In some cases, you may find yourself submitting many queries in a row, which has noticeable latency due to waiting for every query to finish before submitting the next query. There are many different approaches Snowflake allows you to use to submit many queries together.

### Submit Many Queries in Sequence

If you need many queries to be ran in a specific order, then use the pipe operator. Simply put `->>` between your queries and run them.

```sql
{% call statement('main') %}
    alter table {{ this }} cluster by({{ cluster_by }})
    ->> alter table {{ this }} set comment = '{{ comment }}'
{% endcall %}
```

### Submit Many Queries Simultaneously

If you want to submit many queries simultaneously and have all of them run at the same time, then use Snowflake scripting with async queries.

```sql
{% set query %}
    begin
        {# Run queries using the temporary relations. #}
        {% for tenant in tenants %}
        {# Create a relation for each tenant. #}
        {% set tenant_relation = this.incorporate(path={'identifier': this.identifier ~ '_' ~ tenant}) %}
        async(
            create or replace temporary table {{ tenant_relation }} as
                select * from ({{ sql }}) where tenant = '{{ tenant }}'
        );
        {% endfor %}

        {# Wait for all queries to finish. #}
        await all;

        {# Main query. #}
        ...

        {# Cleanup the temporary relations. #}
        {% for tenant in config.require('tenants') %}
        {# Create a relation for each tenant. #}
        {% set tenant_relation = this.incorporate(path={'identifier': this.identifier ~ '_' ~ tenant}) %}
        async(drop table if exists {{ tenant_relation }});
        {% endfor %}

        {# Wait for all queries to finish. #}
        await all;
    end
{% endset %}

{% call statement('main') %}
    execute immediate '{{ escape_ansii(query) }}'
{% endcall %}
```
