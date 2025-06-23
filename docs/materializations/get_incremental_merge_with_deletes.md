# Incremental Merge with Deletes

DBT's incremental strategies do not currently support deletes.
Workarounds involve complex custom strategies to work around this.
The purpose of this incremental strategy is to enable incremental
models with deletes without additional DBT features like post-hooks.
This will eventually be used in conjunction with Snowflake change
tracking features like streams.

## Syntax

### DBT Model

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge_with_deletes',
    unique_key=['id'],
    merge_action=['merge_action']
) }}

with
    {%- if is_incremental() %}
    inserts as (select 3 as id, 'c' as val),
    updates as (select 2 as id, 'x' as val),
    deletes as (select 1 as id, null as val),

    {%- else %}
    inserts as (
        select 1 as id, 'a' as val
        union all
        select 2 as id, 'b' as val
    ),

    {%- endif %}

    merge_with_deletes as (
        select *, 'insert' as merge_action from inserts

        {%- if is_incremental() %}
        union all
        select *, 'update' as merge_action from updates
        union all
        select *, 'delete' as merge_action from deletes
        {%- endif %}
    )

select * from merge_with_deletes
```

### Full-Refresh

```sql
create or replace transient table {{ this }} as
    with
        inserts as (
            select 1 as id, 'a' as val
            union all
            select 2 as id, 'b' as val
        ),

        merge_with_deletes as (
            select *, 'insert' as merge_action from inserts
        )

    select * from merge_with_deletes;
```

### Incremental

```sql
create or replace temporary view {{ dbt_temp }} as
    with
        inserts as (select 3 as id, 'c' as val),
        updates as (select 2 as id, 'x' as val),
        deletes as (select 1 as id, null as val),

        merge_with_deletes as (
            select *, 'insert' as merge_action from inserts
            union all
            select *, 'update' as merge_action from updates
            union all
            select *, 'delete' as merge_action from deletes
        )

    select * from merge_with_deletes;

merge into
    {{ this }} as DBT_INTERNAL_DEST
using
    {{ dbt_temp }} as DBT_INTERNAL_SOURCE
on
    DBT_INTERNAL_DEST.id is not distinct from DBT_INTERNAL_SOURCE.id
when not matched and DBT_INTERNAL_SOURCE.merge_action = 'insert' then
    insert (
        "ID",
        "VAL",
        "MERGE_ACTION"
    ) values (
        DBT_INTERNAL_SOURCE."ID",
        DBT_INTERNAL_SOURCE."VAL",
        DBT_INTERNAL_SOURCE."MERGE_ACTION"
    )
when matched and DBT_INTERNAL_SOURCE.merge_action = 'update' then
    update set
        "ID" = DBT_INTERNAL_SOURCE."ID",
        "VAL" = DBT_INTERNAL_SOURCE."VAL",
        "MERGE_ACTION" = DBT_INTERNAL_SOURCE."MERGE_ACTION"
when matched and DBT_INTERNAL_SOURCE.merge_action = 'delete' then
    delete;
```

## Parameters

### Unique Key: string or list[string]

The unique key used to update and delete on.

### Merge Action: string

The column used to check if a row should be inserted, updated, or deleted.
