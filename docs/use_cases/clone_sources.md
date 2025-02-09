# Clone Sources

Ever have complications due to changing source
data during your DBT jobs?

Clone your sources to get a zero-copy snapshot
of your source data at the start of your DBT
jobs!

```sql
{{ config(materialized='clone_table') }}

clone {{ source('my_source', 'my_table') }}
```
