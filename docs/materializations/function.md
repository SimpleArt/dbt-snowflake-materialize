# Functions

User-defined functions (UDFs) allow you to extend the
capabilities of your SQL models by injecting it with
non-SQL code. This can range from Python machine
learning libraries to API calls to simple JSON
manipulation that are not natively supported on
Snowflake.

There are problems with creating functions directly in
your database, as there are for creating views directly
in your database instead of by using a tool such as DBT.
For example, lack of lineage can make it hard to make
sure changes to functions do not break any downstream
dependencies.

## Examples

### SQL

```sql
{{ config(
    materialized='function',
    parameters='x int, y int',
    returns='int'
) }}

x + y
```

### Python

```sql
{{ config(
    materialized='function',
    parameters='x int, y int',
    returns='int',
    function_config=[
        'language python',
        {'runtime_version': '3.11'},
        {'handler': 'add_int'}
    ]
) }}

def add_int(x, y):
    return x + y
```

## Parameters

### Aggregate, default false

Specifies the function as an aggregation function.

### Parameters, default none

Specifies the parameters for the function, if any.

### Returns

Specifies the return type of the function, such as `varchar` or `table()`.

### Function Config, default none

Specifies additional configurations for the function that are language dependent.

Use a list of configuration objects based on the configurations needed.

#### Config Block

```sql
{{ config(
    materialized='function',
    ...,
    function_config=[
        'key0',
        {'key1': 'value1'},
        {'key2': config_string('value2')},
        {'key3': config_tuple(['value3.1', config_string('value3.2)])},
        {'key4': config_ref('value4')},
        {'key5': config_source('value5.1', 'value5.2')},
        {'key6': config_string(config_source('value6.1', 'value6.2', file='value6.3'))}
    ]
) }}
```

#### Compiled

```sql
create or replace function {{ this }}(...)
    returns ...
    key0
    key1 = value1
    key2 = 'value2'
    key3 = (value3.1, 'value3.2')
    key4 = {{ ref('value4') }}
    key5 = {{ source('value5.1', 'value5.2') }}
    key6 = '{{ source('value6.1', 'value6.2') }}/value6.3'
as ...
```

### Secure, default false

Specifies that potentially sensitive information related to the function is limited to owners of the function, such as the function body (handler).

### Copy Grants, default false

Specifies to keep existing grants if the function needs to be replaced.
