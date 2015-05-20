# DynamoDB configuration and table namespacing

This repository is a collection of small functions that build on top of [boto's dynamodb2 layer](https://boto.readthedocs.org/en/latest/ref/dynamodb2.html) to encourage better configuration and per-environment tables.

Here's a bullet-point summary:

* parses table configuration as defined in a YAML file (see [tests/dynamodb.yml](tests/dynamodb.yml))
* namespaces tables so you can share the same configuration between different environments
* gives you `Table` objects that have the schema and indexes loaded locally so you avoid extra lookups
* direct calls to create or update tables by name as the configuration changes
* optional ability to define non-indexed columns and types of data you expect to store

[![](https://ci.solanolabs.com/Clearcare/cc_dynamodb/badges/branches/master?badge_token=dd4200df12c77f012ea06e70a1c0d0c667b179fe )](https://ci.solanolabs.com/Clearcare/cc_dynamodb/suites/220215)

## Example usage:

```python
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
    | update_table             | Handles updating primary index and global secondary indexes.  |
    |                          | Updates throughput and creates/deletes indexes.               |
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

If you want to use [DynamoDB Local](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html), just pass `host` as a parameter in the connection, e.g.:

    DATABASE = dict(
        namespace='test_',
        host='localhost',
        table_config=DYNAMODB_CONFIG_PATH,
        aws_access_key_id='test',
        aws_secret_access_key='secret',
    )

This uses AWS's provided jar file to run DynamoDB locally. Read more [here](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html).

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

For a tutorial on boto's `dynamodb2` interface, please see [their tutorial](https://boto.readthedocs.org/en/latest/dynamodb2_tut.html).

# TODO:

* Allow external repos to mock without needingo install `moto`
* Fix moto's lack of support for GlobalSecondaryIndex (in metadata, see `test_update_table.py`)
