# Time Travel

Ever need to check what was in a table yesterday?

Don't overcomplicate using DBT snapshots. Use
persistent tables instead to unlock the power of
Snowflake time travel!

```sql
{{ config(materialized='persistent_table') }}

select * from {{ ref('my_table') }}
```

Quickly query historical data based on your Snowflake
data retention period.

```sql
-- Data as of yesterday.
select * from {{ ref('my_persistent_table') }} at(offset => -24 * 60 * 60)
```
