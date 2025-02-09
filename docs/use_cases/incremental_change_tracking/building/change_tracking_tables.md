# Building Tables with Change Tracking

Change tracking is not always enabled. Some common
examples include:

1. External Sources: Your sources may include Snowflake
data shares that do not have change tracking enabled.
2. Intermediate DBT Models: You may want to build
incremental models off of other DBT models. Unless your
DBT model is an incremental model, it does not support
change tracking.
3. Views: Change tracking on views have restrictions on
allowed operations.
4. Dynamic Tables, Hybrid Tables, & Materialized Views:
Change tracking is not available for these types.

Change tracking is not always appropriate to enable.
Some common examples include:

1. Full Refresh Tables: If your table is
dropped/truncated and recreated/reloaded regularly e.g.
your ETL can not perform an incremental load for a table,
then using change tracking will likely result in worse
performance.

To materialize queries into a table incrementally,
accurately, and easily, we provide a powerful
materialization that can be used to support change
tracking:

```sql
{{ config(
    materialized='persistent_table',
    persist_strategy='delete+insert'
) }}

select * from {{ source('my_source', 'my_table') }}
```

Let's look at what this materialization does:

1. Adds `hash(*) as metadata$checksum`.
2. Drops and recreates the table if the schema has changed.
3. Compares `metadata$checksum` against what is currently in the table.
4. Deletes `metadata$checksum` if it no longer exists.
5. Inserts `metadata$checksum` if it doesn't already exist.

The core of this materialization is that the user does
not need to do anything for the table to be loaded into
incrementally, which is done by comparing hashes of the
entire row to find which rows should be deleted and which
rows should be inserted.

Performance-wise, this works sufficiently well for
reasonably sized tables on Snowflake and can be used if
no reliable unique key or check columns can be provided.

If a unique key can be provided, then updates instead of
deletes and inserts can be used instead. Due to the way
Snowflake handles batch DML, this may be more efficient
because a single merge query containing all inserts,
updates, and deletes is performed. In contrast, the
delete+insert strategy performs 1 delete query and 1
insert query. Details at the end for why.

```sql
{{ config(
    materialized='persistent_table',
    unique_key=['unique_key']
) }}

select * from {{ source('my_source', 'my_table') }}
```

The disadvantage of the above 2 configurations is that
computing the `metadata$checksum` requires hashing all
columns from the source. This can be especially
expensive on really wide tables where loading all
columns results in local or remote disk spilling in
your query profile.

If an alternative column can be used instead of
`metadata$checksum`, such as an `updated_at` timestamp
that changes every time the provided unique key has
been updated, then you may be able to improve the
performance of this materialization by including it in
the configurations.

```sql
{{ config(
    materialized='persistent_table',
    unique_key=['unique_key'],
    check_cols=['updated_at']
) }}

select * from {{ source('my_source', 'my_table') }}
```

## Null-Safe

This materialization is null-safe. Null unique keys
and check columns are treated as unique values like
non-null values.

## Duplicate-Safe

This materialization is duplicate-safe.

1. If the unique key is provided and unique, then it
is used to perform the merge.
2. If the unique key is provided and not unique, but
the unique key + check cols are unique, then both are
used to perform the merge.
3. If it is still unique, then changed unique keys +
check cols are deleted and then inserted.

When performing a merge, Snowflake will error (default
behavior) when duplicates are found in the join condition.

When performing deletes, all matched records will be
deleted even if there are multiple matches to the
same join condition.

When performing inserts, all records are inserted without
checking for any keys.
