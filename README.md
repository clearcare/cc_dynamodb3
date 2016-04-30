# DynamoDB configuration and table namespacing

This repository is a collection of small functions that build on top of [boto3's dynamodb](https://boto3.readthedocs.org/en/latest/guide/dynamodb.html) to encourage better configuration and per-environment tables.

Here's a bullet-point summary:

* provides a convenient model interface to query, scan, create, update and delete items
* boto3 integration with conversion between dynamodb and pythonic data
* parses table configuration as defined in a terraform file (see [tests/dynamo_tables.tf](tests/dynamo_tables.tf))
* namespaces tables so you can share the same configuration between different environments
* gives you `Table` objects that have the schema and indexes loaded locally so you avoid extra lookups
* optional ability to define non-indexed columns and types of data you expect to store

**TODO: add back to Solano. Old cc_dynamodb was running.**

## Example usage:

Models:

```python
from cc_dynamodb3.models import DynamoDBModel

from schematics import types as fields

class TestModel(DynamoDBModel):
    TABLE_NAME = 'test'

    agency_subdomain = fields.StringType(required=True)
    external_id = fields.IntType()
    name = fields.StringType()
    is_enabled = fields.BooleanType()


cc_dynamodb3.set_config(
    'path/to/file.tf',
    aws_access_key_id='<KEY>',
    aws_secret_access_key='<SECRET>',
    namespace='dev_',
)

obj = TestModel.create(agency_subdomain='test')  # calls PutItem
obj.is_enabled = True
obj.save()                                       # calls UpdateItem

for obj in TestModel.all():
    print(obj.agency_subdomain)  # prints 'test'

```
And configuration:

https://www.terraform.io/docs/providers/aws/r/dynamodb_table.html

Plain:

```python
import cc_dynamodb3

cc_dynamodb3.set_config(
    'path/to/file.tf',
    aws_access_key_id='<KEY>',
    aws_secret_access_key='<SECRET>',
    namespace='dev_',
)

table = cc_dynamodb3.table.get_table('employment_screening_reports')
    # Returns the boto Table object
    # after figuring out the DynamoDB table name (via namespace)
    # and loading the schema and indexes from the config.
table.scan()  # Table uses boto3 interface
```

# API

## Configuration

Configuration is currently stored globally as a variable and otherwise not cached. So you can think of it as per-thread level caching. There are two functions:

### `get_config(**kwargs)`

Returns the cached config. Calls `set_config` first if no cached config was found.

### `set_config(**kwargs)`

Loads up the terraform configuration file and validates dynamodb connection details. The following are required, either set through the environment, or passed in as kwargs (to overwrite):

* `namespace`, determines the table name prefix. Each repository using this library should have a unique namespace.
* `aws_access_key_id` and `aws_secret_access_key`, the AWS connection credentials for boto's connection. Examples shown in [the tutorial](https://boto3.readthedocs.org/en/latest/guide/quickstart.html#configuration)
* `table_config`, a path to the terraform file for table configuration.

### dynamo_tables.tf

This file contains the table schema for each table (required), and optional secondary indexes (`global_indexes`  or indexes (local secondary indexes).

### `set_redis_config(host='localhost', port=6379, db=3)`

The headline is an example call. Redis caching is optional, but may greatly speed up your server performance.

Redis caching is used to avoid parsing the config file every time `set_config()` is called.

## Usage

The following are all at the `cc_dynamodb3` top level. With the exception of `get_reverse_table_name`, you should always use the unprefixed table name (exactly as from the configuration file).

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
    | query_table              | Provides a nicer interface to query a table than boto3        |
    |                          | default.                                                      |
    |------------------------------------------------------------------------------------------|
    | get_table                | Returns a dict with table and preloaded schema, plus columns. |
    |------------------------------------------------------------------------------------------|
    | list_table_names         | List known table names from configuration, without namespace. |
    |------------------------------------------------------------------------------------------|

## Mocks: `cc_dynamodb3.mocks`

This file provides convenient functions for testing with boto3's `dynamodb`.

You can do most of the testing using the [moto](https://github.com/spulec/moto) library directly, but some things are not supported (mainly, global secondary indexes and querying via them).

Paul's [fork of moto](https://github.com/pcraciunoiu/moto) supports querying, and there is a [Pull Request](https://github.com/spulec/moto/pull/486) open to merge it upstream.

### `mock_table_with_data`

Create a table and populate it with array of items from data.

Example:

    data = [{'key_1': 'value 1'}, {'key_1': 'value 2'}]
    table = mock_table_with_data('some_table', data)
    # Expect 2 results
    assert len(table.scan()) == 2

# Quickstart

In your configuration file, e.g. `config.py`:

    DYNAMODB_TABLE_TF = os.path.join(os.path.dirname(__file__), 'dynamodb_table.tf')
    DATABASE = dict(
        DYNAMODB_TABLE_TF,
        namespace='test_',
        aws_access_key_id='test',
        aws_secret_access_key='secret',
    )

If you want to use [DynamoDB Local](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html), just pass `host` as a parameter in the connection, e.g.:

    DATABASE = dict(
        DYNAMODB_TABLE_TF,
        namespace='test_',
        host='localhost',
        aws_access_key_id='test',
        aws_secret_access_key='secret',
    )

This uses AWS's provided jar file to run DynamoDB locally. Read more [here](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html).

In your database file, e.g. `db.py`:

    import cc_dynamodb3
    dynamodb_config = config.DATABASE
    cc_dynamodb3.set_config(**dynamodb_config)

Then you can use the library directly:

    import cc_dynamodb3

    table = cc_dynamodb3.get_table('some_table')
    item = table.get_item(Key={'some_key': 'value'})

## Dynamodb Tutorial

For more on boto3's `dynamodb` interface, please see [their guide](https://boto3.readthedocs.org/en/latest/guide/dynamodb.html).

## Run the tests

```
$ py.test tests
```
