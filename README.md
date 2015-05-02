# Clearcare DynamoDB common configuration

<!-- [![](https://ci.solanolabs.com:443/Clearcare/cc_dynamodb/badges/170035.png?badge_token=b6bcffb6661e4901c3046ae459eda6a3e0f2fce9)](https://ci.solanolabs.com:443/Clearcare/cc_payment_service/suites/170035) -->

Example usage:

```python
import random
from cc_dynamodb import cc_dynamodb

cc_dynamodb.set_config(
    aws_access_key_id='<KEY>',
    aws_secret_access_key='<SECRET>',
    namespace='dev_')

table = cc_dynamodb.get_table('employment_screening_reports')
    # Returns the boto Table object
    # after figuring out the DynamoDB table name (via namespace)
    # and loading the schema and indexes from the config.
table.scan()  # Table uses boto dynamodb2 interface
```

# API

## Configuration

Configuration is currently stored globally as a variable and otherwise not cached. So you can think of it as per-thread level caching. There are two functions:

### `get_config(**kwargs)`

Returns the cached config. Calls `set_config` first if no cached config was found.

### `set_config(**kwargs)`

Loads up the YAML configuration file and validates dynamodb connection details. The following are required, either set through the environment, or passed in as kwargs (to overwrite):

* `namespace`, determines the table name prefix. Each repository using this library should have a unique namespace.
* `aws_access_key_id` and `aws_secret_access_key`, the AWS connection credentials for boto's connection. Examples shown in [the tutorial](http://boto.readthedocs.org/en/latest/dynamodb2_tut.html)
* `table_config`, a path to the YAML file for table configuration.

### dynamodb.yml

This file contains the table schema for each table (required), and optional secondary indexes (`global_indexes`  or indexes (local secondary indexes).

## Usage

The following are all at the `cc_dynamodb` top level. With the exception of `get_reverse_table_name`, you should always use the unprefixed table name (exactly as from the configuration file).

    |------------------------------------------------------------------------------------------|
    | Function name            | Docstring                                                     |
    |------------------------------------------------------------------------------------------|
    | get_table_name           | Given a table name, return the prefixed version.              |
    |------------------------------------------------------------------------------------------|
    | get_reverse_table_name   | Given a namespaced table name, return the unprefixed version. |
    |------------------------------------------------------------------------------------------|
    | get_table_index          | Given a table name and an index name, return the index.       |
    |------------------------------------------------------------------------------------------|
    | get_connection           | Returns a DynamoDBConnection even if credentials are invalid. |
    |------------------------------------------------------------------------------------------|
    | get_table_columns        | Return known columns for a table and their data type.         |
    |------------------------------------------------------------------------------------------|
    | get_table                | Returns a dict with table and preloaded schema, plus columns. |
    |------------------------------------------------------------------------------------------|
    | list_table_names         | List known table names from configuration, without namespace. |
    |------------------------------------------------------------------------------------------|
    | create_table             | Create table. Throws an error if table already exists.        |
    |------------------------------------------------------------------------------------------|

## Mocks: `cc_dynamodb.mocks`

This file provides convenient functions for testing with `dynamodb2`.

You can do most of the testing using the [moto](https://github.com/spulec/moto) library directly, but some things are not supported (mainly, indexes), or other things are inconvenient.

### `mock_query_2`

If you plan to test querying by secondary indexes, you want to use this. Can be used as a decorator or a context manager (aka via the `with` statement). Similar to how you would use `moto`'s `mock_dynamodb2`.

Example:

    with mock_query_2():
        items = table.query_2(some_column__eq='value', index='SomeColumnIndex')
        
### `mock_table_with_data`

Create a table and populate it with array of items from data.

Example:

    data = [{'key_1': 'value 1'}, {'key_1': 'value 2'}]
    table = mock_table_with_data('some_table', data)
    # Expect 2 results
    assert len(table.scan()) == 2

# Quickstart

In your configuration file, e.g. `config.py`:

    DYNAMODB_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'dynamodb.yml')
    DATABASE = dict(
        namespace='test_',
        table_config=DYNAMODB_CONFIG_PATH,
        aws_access_key_id='test',
        aws_secret_access_key='secret',
    )

In your database file, e.g. `db.py`:

    import cc_dynamodb
    dynamodb_config = config.DATABASE
    cc_dynamodb.set_config(**dynamodb_config)

Then you can use the library directly:

    from db import cc_dynamodb
    TABLE_NAME = 'some_table'

    table = cc_dynamodb.get_table(TABLE_NAME)
    item = table.get_item(some_key='value')

## Dynamodb2 Tutorial

For a tutorial on boto's `dynamodb2` interface, please see [their tutorial](boto.readthedocs.org/en/latest/dynamodb2_tut.html).

# TODO:

* Improved logging
    * Use python's builting logging/logger and set logger name
    * Drop logstash and cc_logger dependency. Let logging be configured externally.
    * Use different logging levels where appropriate: error, info, debug
    * Definitely log table namespace and environment in each log call.
* Update tables to match indexes/throughput, if they already exist
* Easier configuration setup (especially for testing)
* Allow external repos to mock without needingo install `moto`
