# Insert Overwrite

This materialization will truncate and insert current records into a table. Compared to DBT's standard table materialization, this materialization enables more Snowflake features includeing time travel, row access policies, query partitioning, and more. Tables are only recreated if changes are made to the configurations or if the query schema has changed.

## Syntax

### DDL

```sql
create or replace [transient] table {{ this }}({{ query_schema }})
    [cluster by]
    [enable schema evolution]
    [data retention time in days]
    [max data extension time in days]
    [change tracking]
    [default DDL collation]
    [copy grants]
    [row access policy]
    [aggregation policy]
    [join policy]
    [tags]
    [contacts]
```

### DML

```sql
insert overwrite into {{ this }}({{ query_columns }})
    {{ sql }}
```

### Config Block

```sql
{{ config(
    materialized='insert_overwrite',
    transient=true,
    cluster_by=['cluster_by_column'],
    enable_schema_evolution=false,
    data_retention_time_in_days=7,
    max_data_extension_time_in_days=14,
    change_tracking=false,
    default_DDL_collation='collation',
    copy_grants=true,
    row_access_policy={
        'policy': config_ref('row_access_policy'),
        'columns': ['row_access_column']
    },
    aggregation_policy={
        'policy': config_ref('aggregation_policy'),
        'entity_keys': ['entity_key']
    },
    join_policy={
        'policy': config_ref('join_policy'),
        'allowed_join_keys': ['join_key']
    },
    tags=[
        {'tag': config_ref('tag'), 'value': 'tag_value'}
    ],
    contacts={
        'approver': config_ref('contact')
    }
    partition_by={
        'from': 'example_cte',
        'columns': ['partition_by_column'],
        'partitions': 10,
        'async': true
    }
) }}

with
    example_cte as (select * from {{ ref('example_table') }} {{ partition_filter() }})

select
    cluster_by_column,
    row_accss_column,
    entity_key,
    join_key,
    partition_by_column
from
    example_cte
```

### YAML

```yaml
# dbt_project.yml

models:
  project_name:
    model_name:
      materialized: insert_overwrite
      transient: true
      cluster_by:
        - columns
      enable_schema_evolution: false
      data_retention_time_in_days: 7
      max_data_extension_time_in_days: 14
      change_tracking: false
      default_ddl_collaton: 'collation'
      copy_grants: true
      row_access_policy:
        policy: {{ ref('row_access_policy') }}
        columns:
          - row_access_column
      aggregation_policy:
        policy: {{ ref('aggregation_policy') }}
        entity_keys:
          - entity_key
      join_policy:
        policy: {{ ref('join_policy') }}
        allowed_join_keys:
          - join_key
      tags:
        - tag: {{ ref('tag') }}
          value: tag_value
      contacts:
        approver: {{ ref('contact') }}
      partition_by:
        from: example_table
        columns:
          - partition_by_column
        partitions: 10
        async: true
```

## Required Configurations

This materialization has no required configurations.

## Optional Configurations

### Transient, default false

Specifies if the table should be created as a transient table. By default, false to enable longer data retention time with permanent tables.

### Cluster By, default none

Specifies what to cluster the data by. This can help improve search and join performance on some columns. Not as useful for intermediate models. Consider using `order by` at the end of your select query instead.

If no `cluster_by` is defined, then no cluster by is applied or unset.

If `cluster_by = []`, then any existing cluster by is unset.

### Enable Schema Evolution, default false

Specifies if files can be uploaded into this table (outside of DBT) using schema evolution.

### Data Retention Time In Days, default none

Specifies how many days data can be retained for Snowflake time travel. Extremely useful for debugging what the data used to be in a table. Recommended lower for intermediate models and higher for models with higher visibility.

If no `data_retention_time_in_days` is specified, Snowflake will use the default data retention time set on the account.

### Max Data Extension Time In Days, default none

Specifies how many days data can be retained beyond the data extension time for streams.

If no `max_data_extention_time_in_days` is specified, Snowflake will use the default max data extension time set on the account.

### Change Tracking, default false

Flags if a table should enable change tracking for streams. Not recommended for tables using the `insert_overwrite` materialization.

### Default DDL Collation, default none

Specifies the default collation set for text columns, such as case insensitivity.

If no `default_ddl_collaton` is specified, then text columns are compared exactly unless otherwise specified.

### Copy Grants, default false

Flags if a table should retain prior grants any time it gets replaced. Useful if you want to manage grants manually per table instead of with future grants.

### Row Access Policy, default none

Specifies a row access policy to filter rows out of this table based on the current user's access to the data.

Note: when using a config block, use the `config_ref()` and `config_source()` macros instead of the `ref()` and `source()` macros.

### Aggregation Policy, default none

Specifies an aggregation policy to require aggregations on this table based on the current user's access to the data.

Note: when using a config block, use the `config_ref()` and `config_source()` macros instead of the `ref()` and `source()` macros.

### Join Policy, default none

Specifies a join policy to require joins on specific columns to this table based on the current user's access to the data.

Note: when using a config block, use the `config_ref()` and `config_source()` macros instead of the `ref()` and `source()` macros.

### Tags, default none

Specifies tags to apply to this table.

Note: when using a config block, use the `config_ref()` and `config_source()` macros instead of the `ref()` and `source()` macros.

### Contacts, default none

Specifies contacts to apply to this table.

Note: when using a config block, use the `config_ref()` and `config_source()` macros instead of the `ref()` and `source()` macros.

### Partition By, default none

Specifies columns to partition the query by. This can significantly help with performance for any queries which may use a significant amount of memory on intermediate steps, such as expensive aggregations or windows.

The idea behind using query partitioning is to split the query into several partitions and run each separately. This allows each separate query to run through the intermediate steps faster by consuming less memory per query, and before all of the available memory on the warehouse clusters get used up, the intermediate step in the query is completed and moved on to other steps.

Example of what query partitioning may look like:

#### Model

```sql
{{ config(
    materialized='insert_overwrite',
    partition_by={
        'from': 'example_cte',
        'columns': ['partition_by_column']
    }
) }}

with
    example_cte as (select * from {{ ref('example_table') }} {{ partition_filter() }})

select * from example_cte
```

#### DML

```sql
truncate table {{ this }};

insert into {{ this }}
    with
        table_1 as (select * from {{ ref('table_1') }} where state = 'IL'),
        table_2 as (select * from {{ ref('table_2') }} where state = 'IL')

    select
        state,
        table_1.x,
        table_2.y,
        sum(table_2.z) as z
    from
        table_1
    natural inner join
        table_2
    group by
        all;

insert into {{ this }}
    with
        table_1 as (select * from {{ ref('table_1') }} where state = 'FL'),
        table_2 as (select * from {{ ref('table_2') }} where state = 'FL')

    select
        state,
        table_1.x,
        table_2.y,
        sum(table_2.z) as z
    from
        table_1
    natural inner join
        table_2
    group by
        all;
```

#### From, required

For this feature to work, the materialization needs to query for a list of values to build the partitions based off of. Create a CTE (or use an existing CTE) that includes the list of possible partition values.

#### Columns, required

For this feature to work, the materialization needs to know what columns to partition by. This feature requires all tables to use the same columns for filtering. Use CTEs to rename columns as needed.

#### Partitions, default none

Instead of submitting 1 query for every individual partition, submits queries that group multiple partitions together.

Default: 1 query submitted for every partition.

#### Async, default false

Flags if queries should be submitted asynchronously. This allows DBT to send multiple queries at the same time, allowing more threads to get used on the warehouse. This may cause other DBT models running at the same time to get blocked by this feature.
