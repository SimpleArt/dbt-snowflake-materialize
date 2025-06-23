# Procedures

Originally, stored procedures were built out using on-run-end hooks.
The purpose of this materialization is to enable the creation of
stored procedures using actual DBT models. This enables dependency
tracking, which resolves some of the build problems we've experienced
when using on-run-end hooks.

Stored procedures are persistent. Stored procedures are not dropped
unless changes have been made to them, in hopes to prevent jobs
failing from trying to rebuild stored procedures and getting hung up.

## Examples

### SQL

```sql
{{ config(
    materialized='stored_procedure',
    parameters='database_name varchar',
    returns='table()'
) }}

begin
    let res resultset := (show tables in database identifier(:database_name));
    return table(res);
end
```

### Python

```sql
{{ config(
    materialized='stored_procedure',
    parameters='database_name varchar',
    returns='table()',
    procedure_config=[
        'language python',
        {'runtime_version': '3.11'},
        {'packages': config_tuple([config_string('snowflake-snowpark-python')])},
        {'handler': config_string('show_tables')}
    ]
) }}

def return_input(session, database_name):
    return session.sql(f"show tables in database {database_name}")
```

## Parameters

### Parameters, default none

Specifies the parameters for the procedure, if any.

### Returns

Specifies the return type of the procedure, such as `varchar` or `table()`.

### Procedure Config, default none

Specifies additional configurations for the procedure that are language dependent.

Use a list of configuration objects based on the configurations needed.

#### Config Block

```sql
{{ config(
    materialized='procedure',
    ...,
    procedure_config=[
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
create or replace procedure {{ this }}(...)
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

Specifies that potentially sensitive information related to the procedure is limited to owners of the procedure, such as the procedure body (handler).

### Copy Grants, default false

Specifies to keep existing grants if the procedure needs to be replaced.
