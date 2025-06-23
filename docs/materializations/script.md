# Scripts

Sometimes, DBT's approach to building data pipelines is too rigid for what you need. Maybe you want to create a materialized view in Snowflake and just really wished you could write `create materialized view if not exists {{ this }} as ...`. Maybe you want to utilize temporary tables. Maybe you want to run many different queries simultaneously using Snowflake's `async` capabilities. Whatever the reason, we believe that enabling you to write what you want in DBT directly will be better than writing it outside of DBT, where you will lose lineage, documentation, version control, and so on.

## Why Not Pre-Hooks/Post-Hooks?

Fundamentally, scripts represent an entirely different materialization, where-as pre-hooks and post-hooks are just add-ons to existing materializations. You should use pre-hooks and post-hooks when you just want to run a little extra code before or after the main materialization, whether that be a table, view, function, procedure, etc.

Scripts should be used when you need to go beyond minor additional code on top of your existing materializations. Maybe you want a fundamentally different materialization than what's currently accessible and you don't have the skillsets (nor the need) to create a custom materialization for your specific situation. Maybe the DDL and DML logic you need in this case is very unique and should not be abstracted into a custom materialization.

## Syntax

### Single Query

The simplest use case, just a single query you want to submit.

```sql
{{ config(
    materialized='script',
    script_materialization='table'
) }}

create or alter table {{ this }}(
    order_id int autoincrement start 1 increment 1 order,
    order_date date,
    order_value float
)
```

### Multiple Queries: Pipe Operator

Chain multiple queries together using the pipe operator.

```sql
{{ config(
    materialized='script',
    script_materialization='table'
) }}

create or alter table {{ this }}(
    order_id int autoincrement start 1 increment 1 order,
    order_date date,
    order_value float
)
->> alter table {{ this }} set comment = 'Orders'
```

### Multiple Queries: Anonymous Block

Chain multiple queries using Snowflake scripting.

```sql
{{ config(
    materialized='script',
    script_materialization='table'
) }}

begin
    create or alter table {{ this }}(
        order_id int autoincrement start 1 increment 1 order,
        order_date date,
        order_value float
    );

    async(alter table {{ this }} set comment = 'Orders');

    async(
        insert into {{ this }}(order_date, order_value)
            values(current_date, null)
    );

    await all;
end
```

### Multiple Queries: Other Languages

Chain multiple queries together using a different language.

Uses the same configurations as procedures, except `returns` is not needed unless you want to return a value e.g. for logging purposes.

```sql
{{ config(
    materialized='script',
    script_materialization='table',
    anonymous_procedure=dict(
        procedure_config=[
            'language python',
            {'runtime_version': '3.11'},
            {'packages': config_tuple([config_string('snowflake-snowpark-python')])},
            {'handler': config_string('create_table')}
        ]
    )
) }}

def create_table(session):
    session.sql("""
        create or alter table {{ this }}(
            order_id int autoincrement start 1 increment 1 order,
            order_date date,
            order_value float
        )
    """).collect()

    session.sql("alter table {{ this }} set comment = 'Orders'").collect()

    df = session.read.schema(...).csv("@stage/data.csv")
    df.copy_into_table("{{ this }}", target_columns=..., force=True)
```

## Parameters

### Script Materialization, default none

Specifies what you want your script to materialize, such as "table", "view", "materialized view", etc.

If specified, drops `{{ this }}` if it is not materialized as the desired object type before running the script.

### Drop If, default none

Specifies a custom test for additionally dropping `{{ this }}` if it materialized as the desired object type, but with the wrong metadata.

Examples:

Drop a table if it is transient.

```sql
{% set drop_if %}
select * from $1 where "kind" = 'TRANSIENT'
{% endset %}

{{ config(
    materialized='script',
    script_materialization='table',
    drop_if=drop_if
) }}

create or alter table {{ this }}(
    order_id int autoincrement start 1 increment 1 order,
    order_date date,
    order_value float
)
```

Drop a view if it does not have the right comment.

```sql
{% set drop_if %}
select * from $1 where "comment" != 'v1'
{% endset %}

{{ config(
    materialized='script',
    script_materialization='view',
    drop_if=drop_if
) }}

create or replace recursive view {{ this }}(
    managers array,
    employee_id int
)
    comment = 'v1'
as
    select [], employee_id from {{ ref('employee_managers') }} where manager_id is null

    union all

    select
        array_append(T1.managers, T2.manager_id),
        T2.employee_id
    from
        {{ this }} as T1
    inner join
        {{ ref('employee_managers') }} as T2
            on T1.employee_id = T2.manager_id
```
