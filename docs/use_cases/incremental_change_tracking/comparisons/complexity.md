# Complexity

CDC is simple, standardized, and works without error.

```sql
{{ config(
    materialized='materialized_stream',
    aggregate={'revenue': 'sum'}
) }}

select * from {{ ref('my_stream') }}
```

Modeling with incremental models requires much more
care.

- What if dates/timestamps are not available?
- What happens if data arrives late?
- What happens if records are deleted?
- What happens if my keys have duplicates?
- What happens if my keys or timestamps have nulls?
- What happens if data is loaded in parallel?
- How do I build incremental models that query multiple tables?
- How do I validate that my incremental model is accurately matching expectations?
- How do I build a data model that can be used in downstream incremental models?
- Is the solution data drift just manual bug fixes and full refreshes?

I have seen many different DBT users placed between
a need for performance and a need for quality data
models. The answer? Sacrifice quality for performance
and handle drift using regular full refreshes.
