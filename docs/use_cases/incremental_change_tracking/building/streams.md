# Streams

In order to query the records which have changed,
Snowflake provides a special type of object: streams.
Streams return the records which have changed in their
source data **since the last time they have been
queried**, which can be used to build incremental data
pipelines downstream.

Let's take a look at building streams on tables.

```sql
{{ config(materialized='stream') }}

on table {{ ref('my_persistent_table') }}
```

Streams are full refreshed under any of the following
are true:

1. DBT full refresh is ran.
2. The stream does not already exist.
3. The upstream table is dropped.
4. The stream has become stale or can not be otherwise queried.

Full refreshing causes the existing stream to be
dropped and recreated. When the stream is created,
all current records will be present in the stream as
inserted records until the stream has been queried,
after which only changes since the last query will
be in the stream.
