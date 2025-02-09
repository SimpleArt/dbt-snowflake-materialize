# Streams on Views

Snowflake supports streams on views, which allows you to
apply static filters, inner joins, and union all. This
allows you to manage change tracking across multiple
tables as well as efficiently apply filters to avoid
loading unnecessary data.

For this feature to work, the views created must not be
dropped. For this purpose, we introduce a new custom
materialization: persistent views.

```sql
{{ config(materialized='persistent_view') }}

select * from {{ ref('source_A') }}
union all
select * from {{ ref('source_B') }}
```

Persistent views are the same as regular views, except
that they are full refreshed only if all of the
following are true:

1. DBT full refresh is ran.
2. The view does not already exist.
3. The query has changed.
4. The column schema has changed.

Streams can be created on views instead of tables.

```sql
{{ config(materialized='stream') }}

on view {{ ref('my_persistent_view') }}
```

Streams, especially on views, can be materialized
without any aggregations to just load them into a table.

```sql
{{ config(materialized='materialized_stream') }}

select * from {{ ref('my_stream') }}
```

When creating materialized streams without any
aggregations, use a persistent view to exclude the
`metadata$row_id` column before creating a stream on it.

```sql
{{ config(materialized='persistent_view') }}

select * exclude metadata$row_id from {{ ref('my_materialized_stream') }}
```
