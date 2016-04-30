from functools import partial
import operator

from boto3.dynamodb.conditions import Key, Attr

from .config import get_config
from .connection import get_connection
from .exceptions import UnknownTableException


def get_table_name(table_name):
    """
    Prefixes the table name for the different environments/settings.

    :param table_name: unprefixed table name
    :return: prefixed table name
    """
    return get_config().namespace + table_name


def get_reverse_table_name(table_name):
    """
    Un-prefixes the table name for the different environments/settings.

    :param table_name: prefixed table name
    :return: unprefixed table name
    """
    prefix_length = len(get_config().namespace)
    return table_name[prefix_length:]


def get_table_index(table_name, index_name):
    """Given a table name and an index name, return the index."""
    config = get_config()
    all_indexes = (config.yaml.get('indexes', {}).items() +
                   config.yaml.get('global_indexes', {}).items())
    for config_table_name, table_indexes in all_indexes:
        if config_table_name == table_name:
            for index in table_indexes:
                if index['name'] == index_name:
                    return index


def get_table(table_name, connection=None):
    """Returns a dict with table and preloaded schema, plus columns.

    WARNING: Does not check the table actually exists. Querying against
             a non-existent table raises boto.exception.JSONResponseError

    This function avoids additional lookups when using a table.
    The columns included are only the optional columns you may find in some of the items.
    """
    if table_name not in list_table_names():
        raise UnknownTableException('Unknown table: %s' % table_name)

    dynamodb = connection or get_connection()
    return dynamodb.Table(
        get_table_name(table_name),
    )


def _maybe_table_from_name(table_name_or_class):
    return get_table(table_name_or_class) if isinstance(table_name_or_class, basestring) else table_name_or_class

def query_table(table_name_or_class, query_index=None, descending=False, limit=None,
                exclusive_start_key=None, filter_expression=None, **query_keys):
    """
    Friendly version to query a table using boto3's interface

    :param table_name_or_class: (string) un-prefixed table name
    :param query_index: (string, optional) optionally specify a GSI (Global) or LSI (Local Secondary Index)
    :param descending: (boolean, optional) sort in descending order (default: False)
    :param limit: (integer, optional) limit the number of results directly in the query to dynamodb
    :param exclusive_start_key: (dictionary) resume from the prior query's LastEvaluatedKey
    :param filter_expression: (dictionary, optional) Dictionary of filter attributes, expressed same as query_keys
    :param query_keys: query arguments, syntax: attribute__gte=123 (similar to boto2's interface)
    :return: boto3 query response
    """
    # filter_expression is limited in its expressiveness relative to what boto3 is capable.
    # Only arity 1 conditions are supported, e.g., 'eq', 'gt', 'gte', 'lt', 'lte', 'begins_with', 'contains',
    # 'is_in', 'ne'. Arity 0 conditions ('not_exists', 'size') and arity 2 conditions ('between') are not supported.
    # Multiple expressions are all ANDed together. There is no option for ORing or creating more complex
    # expressions with combinations of AND/OR/NOT.

    keys = []
    for key_name, value in query_keys.items():
        key_name_and_operator = key_name.split('__')
        if len(key_name_and_operator) == 1:
            op = 'eq'
        else:
            key_name = key_name_and_operator[0]
            op = key_name_and_operator[1]

        if isinstance(value, bool):  # Starting boto3, conversion from True to Decimal('1') is not automatic.
            value = int(value)

        keys.append(
            getattr(Key(key_name), op)(value)
        )

    query_kwargs = dict(
        KeyConditionExpression=reduce(operator.and_, keys),
        ScanIndexForward=False if descending else True,
    )

    if filter_expression is not None:
        attrs = []
        for key_name, value in filter_expression.items():
            key_name_and_operator = key_name.split('__')
            if len(key_name_and_operator) == 1:
                op = 'eq'
            else:
                key_name = key_name_and_operator[0]
                op = key_name_and_operator[1]

            if isinstance(value, bool):  # See above
                value = int(value)

            attrs.append(
                getattr(Attr(key_name), op)(value)
            )
        if attrs:
            query_kwargs['FilterExpression'] = reduce(operator.and_, attrs)

    if limit is not None:
        query_kwargs['Limit'] = limit
    if query_index:
        query_kwargs['IndexName'] = query_index
    if exclusive_start_key:
        query_kwargs['ExclusiveStartKey'] = exclusive_start_key

    return _maybe_table_from_name(table_name_or_class).query(**query_kwargs)


def scan_table(table_name_or_class, exclusive_start_key=None, **scan_kwargs):
    if exclusive_start_key:
        scan_kwargs['ExclusiveStartKey'] = exclusive_start_key
    return _maybe_table_from_name(table_name_or_class).scan(**scan_kwargs)


def _retrieve_all_matching(query_or_scan_func, *args, **kwargs):
    """Used by scan/query below."""
    limit = kwargs.pop('limit', None)
    query_or_scan_kwargs = kwargs.copy()
    response = query_or_scan_func(*args, **query_or_scan_kwargs)
    total_found = 0

    # DynamoDB only returns up to 1MB of data per trip, so we need to keep querying or scanning.
    while True:
        metadata = response.get('ResponseMetadata', {})
        for row in response['Items']:
            yield row, metadata
            total_found += 1
            if limit and total_found == limit:
                break
        if limit and total_found == limit:
            break
        if response.get('LastEvaluatedKey'):
            query_or_scan_kwargs['exclusive_start_key'] = response['LastEvaluatedKey']
            response = query_or_scan_func(*args, **query_or_scan_kwargs)
        else:
            break


def scan_all_in_table(table_name_or_class, *args, **kwargs):
    """
    Scan all records in a table. May perform multiple calls to DynamoDB.

    DynamoDB only returns up to 1MB of data per scan, so we need to keep scanning,
    using LastEvaluatedKey.

    :param table_name_or_class: 'some_table' or get_table('some_table')
    :param args: see args accepted by boto3 dynamodb scan
    :param kwargs: see kwargs accepted by boto3 dynamodb scan
    :return: list of records as tuples (row, metadata)
    """
    scan_partial = partial(scan_table, table_name_or_class)
    return _retrieve_all_matching(scan_partial, *args, **kwargs)


def query_all_in_table(table_name_or_class, *args, **kwargs):
    """
    Query all records in a table. May perform multiple calls to DynamoDB.

    DynamoDB only returns up to 1MB of data per query, so we need to keep querying,
    using LastEvaluatedKey.

    :param table_name_or_class: 'some_table' or get_table('some_table')
    :param args: see args accepted by query_table
    :param kwargs: see kwargs accepted by query_table
    :return: list of records as tuples (row, metadata)
    """
    query_partial = partial(query_table, table_name_or_class)
    return _retrieve_all_matching(query_partial, *args, **kwargs)


def list_table_names():
    """List known table names from configuration, without namespace."""
    return get_config().table_config.list_table_names()


def get_table_config(table_name):
    config = get_config()
    init_data = dict(
        TableName=get_table_name(table_name),
    )
    init_data.update(config.table_config.get_table(table_name))
    return init_data
