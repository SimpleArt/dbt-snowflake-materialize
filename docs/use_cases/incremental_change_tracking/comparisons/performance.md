# Performance

For common data modeling patterns (joins, unions,
aggregations), change tracking has standardized ways
to build incremental models. Micropartition filtering
is also taken advantage of because Snowflake is able
to use micropartition metadata when tracking changes.

For less common data modeling patterns (windows, time
joins, etc.), writing custom incremental models may
be needed. In many cases, change tracking can be used
as part of your incremental filters.

The biggest reason to not use change tracking in this
case is if your source data does not change tracking
and incremental logic can be written without change
tracking. Otherwise, there may not be a significant
performance gain either way.
