# Aggregating Streams

Streams can be aggregated using our custom
materialization: materialized streams. There are 4
supported aggregations for streams:

1. `sum`: Inserted records add onto the total, while
deleted records subtract. If any value is non-null
in the history of this model, then this value will
be non-null.
2. `max` and `min`: Inserted records are compared
against the stored value. Deleted records are not
considered.
3. `count_agg`: Custom materialization that aggregates
a count of each distinct non-null value. Inserted
values add 1 and deleted values subtract 1. Several
additional functions exist to extract various results
out of this, such as the `mode`, `median`, etc.

```sql
{{ config(
    materialized='materialized_stream',
    aggregate={
        'revenue': 'sum',
        'product_name': 'count_agg'
    }
) }}

select
    sold_at::date as sold_on,
    revenue,
    product_name,
    -- Stream metadata.
    metadata$action,
    metadata$isupdate,
    metadata$row_id
from
    {{ ref('my_stream') }}
```

Your materialized stream can be queried for the
expected columns, already aggregated for you.

```sql
select
    sold_on,
    revenue,
    product_name['Product A']::int as product_As_sold,
    product_name['Product B']::int as product_Bs_sold
from
    {{ ref('my_materialized_stream') }}
```
