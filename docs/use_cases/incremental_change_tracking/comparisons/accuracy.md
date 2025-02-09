# Accuracy

CDC models don't require checks. Dynamic tables,
materialized views, and streams are backed by
Snowflake to be an accurate representation of what
you model. They provide a standardized way to build
incremental models using record changes recorded
internally by Snowflake, which are used internally by
Snowflake's materialized views and dynamic tables.
Our custom materialization, materialized streams,
simply wraps similar logic to what Snowflake uses
internally and exposes it as tables that can be used
in your DBT project.

Incremental models are not guaranteed to be perfect.
The reliability of your incremental models depends
on a number of factors:

- The complexity of the model.
- The reliability of the source data.
- The experience of the data modeler.

The only reliable way to ensure your incremental
models are accurate are to full refresh them because
full refreshing does not involve incremental logic,
it involves declarative data modeling, and declarative
data modeling is significantly easier to get right.
It's why we all moved to DBT instead of writing stored
procedures.
