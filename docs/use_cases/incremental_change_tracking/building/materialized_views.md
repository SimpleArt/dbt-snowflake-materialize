# Materialized Views

Snowflake supports materialized views, which
are a type of view that attempts to precompute
itself and materialize those results into a
table for faster query performance. In our
experience, the performance of materialized
views can depend greatly on the specific query
and how the underlying table is clustered.
In some cases, the equivalent dynamic table
or materialized stream may be more performant
than using materialized views.

```sql
{{ config(materialized='materialized_stream') }}

select product_name, sum(revenue) as revenue
from {{ ref('my_table') }}
group by product_name
```
