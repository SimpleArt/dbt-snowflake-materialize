# Differences

Let's look at the differences between writing
your own incremental models and using CDC.

|              | Incremental   | CDC                      |
| ------------ | ------------- | ------------------------ |
| Accuracy     | At Risk       | No Risk                  |
| Complexity   | Complex Logic | Declarative              |
| Performance  | At Risk       | Micropartition Filtering |
| Capabilities | Custom Logic  | Union, Join, Aggregate   |

Within CDC, there are also a couple of different
options available. Although dynamic tables,
materialized views, and materialized streams all
use Snowflake change tracking, there are
differences between each option.

|                | Dynamic Table          | Materialized View | Materialized Stream    |
| -------------- | ---------------------- | ----------------- | ---------------------- |
| Scheduling     | Lag                    | Real Time         | DBT Jobs               |
| Capabilities   | Union, Join, Aggregate | Aggregate         | Union, Join, Aggregate |
| Can be Used in | Dynamic Tables         | None              | All                    |
