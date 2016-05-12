from functools import partial
import operator

from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

from .config import get_config
from .connection import get_connection
from .exceptions import (
    TableAlreadyExistsException,
    UpdateTableException,
    UnknownTableException,
)
from .log import log_data


def _build_key_type(key_type):
    if key_type == 'HashKey':
        return 'HASH'
    if key_type == 'RangeKey':
        return 'RANGE'
    raise NotImplementedError('Unknown Key Type: %s' % key_type)


def _build_key_schema(keys_config):
    return [
        {
            'KeyType': _build_key_type(key['type']),
            'AttributeName': key['name'],
        } for key in keys_config
    ]


def _build_attribute_definitions(keys_config):
    return [
        {
            'AttributeName': key['name'],
            'AttributeType': key['data_type'][0],
        } for key in keys_config
    ]


def _build_index_type(index_type):
    # Valid values: 'ALL'|'KEYS_ONLY'|'INCLUDE'
    if index_type not in ('AllIndex', 'GlobalAllIndex'):
        raise NotImplementedError('TODO: support KEYS_ONLY and INCLUDE with Projection')
    return 'ALL'


def _get_table_metadata(table_name):
    config = get_config().yaml

    try:
        keys_config = config['schemas'][table_name]
    except KeyError:
        log_data('Unknown Table',
                 extra=dict(table_name=table_name,
                            config=config),
                 logging_level='exception')
        raise UnknownTableException('Unknown table: %s' % table_name)

    metadata = dict(
        KeySchema=_build_key_schema(keys_config),
        AttributeDefinitions=_build_attribute_definitions(keys_config)
    )

    global_indexes_config = config.get('global_indexes', {}).get(table_name, [])
    indexes_config = config.get('indexes', {}).get(table_name, [])

    if indexes_config:
        lsis = []
        for lsi_config in indexes_config:
            lsis.append({
                'IndexName': lsi_config['name'],
                'KeySchema': [
                    {
                        'AttributeName': part['name'],
                        'KeyType': _build_key_type(part['type']),
                    }
                    for part in lsi_config['parts']
                ],
                'Projection': {
                    'ProjectionType': _build_index_type(lsi_config['type']),
                },
            })
        attributes = []
        for index in indexes_config:
            for attribute in index['parts']:
                attributes.append(attribute)
        metadata['AttributeDefinitions'] += _build_attribute_definitions(attributes)
        metadata.update(LocalSecondaryIndexes=lsis)

    if global_indexes_config:
        gsis = []
        for gsi_config in global_indexes_config:
            formatted = _get_or_default_throughput(gsi_config.get('throughput') or False)
            provisioned_throughput = formatted['ProvisionedThroughput']
            gsis.append({
                'IndexName': gsi_config['name'],
                'KeySchema': [
                    {
                        'AttributeName': part['name'],
                        'KeyType': _build_key_type(part['type']),
                    }
                    for part in gsi_config['parts']
                ],
                'Projection': {
                    'ProjectionType': _build_index_type(gsi_config['type']),
                },
                'ProvisionedThroughput': provisioned_throughput,
            })
        attributes = []
        for index in global_indexes_config:
            for attribute in index['parts']:
                attributes.append(attribute)
        metadata['AttributeDefinitions'] += _build_attribute_definitions(attributes)
        metadata.update(GlobalSecondaryIndexes=gsis)

    # Unique-fy AttributeDefinitions
    attribute_definitions = dict()
    for attribute in metadata['AttributeDefinitions']:
        if (attribute['AttributeName'] in attribute_definitions and
                attribute['AttributeType'] != attribute_definitions[attribute['AttributeName']]['AttributeType']):
            raise ValueError('Mismatched attribute type for %s. Found: %s and %s' %
                             (attribute['AttributeName'], attribute['AttributeType'],
                              attribute_definitions[attribute['AttributeName']]['AttributeType']))
        attribute_definitions[attribute['AttributeName']] = attribute
    metadata['AttributeDefinitions'] = attribute_definitions.values()

    return metadata


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


def get_table_columns(table_name):
    """Return known columns for a table and their data type."""
    config = get_config().yaml
    try:
        return dict(
            (column_name, column_type)
                for column_name, column_type in config['columns'][table_name].items())
    except KeyError:
        log_data('Unknown Table',
                 extra=dict(table_name=table_name,
                            config=config),
                 logging_level='exception')
        raise UnknownTableException('Unknown table: %s' % table_name)


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
    return get_config().yaml['schemas'].keys()


def _get_or_default_throughput(throughput):
    if throughput is False:
        config = get_config()
        throughput = config.yaml['default_throughput']

    if not throughput:
        return dict()
    return dict(
        ProvisionedThroughput=dict(
            ReadCapacityUnits=throughput['read'],
            WriteCapacityUnits=throughput['write'],
        )
    )


def _get_table_init_data(table_name, throughput):
    init_data = dict(
        TableName=get_table_name(table_name),
    )

    init_data.update(_get_table_metadata(table_name))
    init_data.update(_get_or_default_throughput(throughput))

    return init_data


def create_table(table_name, connection=None, throughput=False):
    """Create table. Throws an error if table already exists."""
    dynamodb = connection or get_connection()
    init_data = _get_table_init_data(table_name, throughput=throughput)
    try:
        db_table = dynamodb.create_table(**init_data)
        if isinstance(db_table, dict):
            # We must be using moto's crappy mocking
            db_table = dynamodb.Table(init_data['TableName'])

        db_table.meta.client.get_waiter('table_exists').wait(TableName=init_data['TableName'])
        log_data('create_table: %s' % table_name,
                 extra=dict(status='created table',
                            table_name=table_name),
                 logging_level='info')
        return db_table
    except ClientError as e:
        if (e.response['ResponseMetadata']['HTTPStatusCode'] == 400 and
                e.response['Error']['Code'] == 'ResourceInUseException'):
            log_data('Called create_table("%s"), but already exists: %s' %
                     (table_name, e.response),
                     extra=dict(table_name=table_name),
                     logging_level='warning')
            raise TableAlreadyExistsException(response=e.response)
        raise e


def _validate_schema(table_name, upstream_schema, local_schema):
    """Raise error if primary index (schema) is not the same as upstream"""
    if sorted(upstream_schema, key=lambda i: i['AttributeName']) != sorted(local_schema, key=lambda i: i['AttributeName']):
        msg = 'Mismatched schema: %s VS %s' % (upstream_schema, local_schema)
        log_data(msg, logging_level='warning')
        raise UpdateTableException(msg)


def update_table(table_name, connection=None, throughput=False):
    """
    Update existing table.

    Handles updating primary index and global secondary indexes.
    Updates throughput and creates/deletes indexes.

    :param table_name: unprefixed table name
    :param connection: optional dynamodb connection, to avoid creating one
    :param throughput: a dict, e.g. {'read': 10, 'write': 10}
    :return: the updated boto Table
    """
    db_table = get_table(table_name, connection=connection)
    try:
        db_table.load()
    except ClientError as e:
        if (e.response['ResponseMetadata']['HTTPStatusCode'] == 400 and
                e.response['Error']['Code'] == 'ResourceNotFoundException'):
            raise UnknownTableException('Unknown table: %s' % table_name)

    local_metadata = _get_table_metadata(table_name)
    _validate_schema(table_name, upstream_schema=db_table.key_schema, local_schema=local_metadata['KeySchema'])

    # Update existing primary index throughput
    if throughput:
        formatted = _get_or_default_throughput(throughput)
        if formatted['ProvisionedThroughput'] != db_table.provisioned_throughput:
            db_table.update(**formatted)

    local_global_indexes_by_name = dict((i['IndexName'], i) for i in local_metadata.get('GlobalSecondaryIndexes', []))
    upstream_global_indexes_by_name = dict((i['IndexName'], i) for i in (db_table.global_secondary_indexes or []))

    gsi_updates = []

    for index_name, index in local_global_indexes_by_name.items():
        if index_name not in upstream_global_indexes_by_name:
            log_data('Creating GSI %s for %s' % (index_name, table_name),
                     logging_level='info')
            gsi_updates.append({
                'Create': index,
            })
        else:
            upstream_index = upstream_global_indexes_by_name[index_name]
            if (index['ProvisionedThroughput'].get('ReadCapacityUnits') ==
                    upstream_index['ProvisionedThroughput'].get('ReadCapacityUnits')) and \
               (index['ProvisionedThroughput'].get('WriteCapacityUnits') ==
                    upstream_index['ProvisionedThroughput'].get('WriteCapacityUnits')):
                continue
            gsi_updates.append({
                'Update': {
                    'IndexName': index_name,
                    'ProvisionedThroughput': index['ProvisionedThroughput']
                },
            })
            log_data('Updating GSI %s throughput for %s to %s' % (index_name, table_name, index['ProvisionedThroughput']),
                     logging_level='info')

    for index_name in upstream_global_indexes_by_name.keys():
        if index_name not in local_global_indexes_by_name:
            log_data('Deleting GSI %s for %s' % (index_name, table_name),
                     logging_level='info')
            gsi_updates.append({
                'Delete': {
                    'IndexName': index_name,
                }
            })

    if gsi_updates:
        db_table.update(AttributeDefinitions=local_metadata['AttributeDefinitions'],
                        GlobalSecondaryIndexUpdates=gsi_updates)
        log_data('update_table: %s' % table_name, extra=dict(status='updated table',
                                                             table_name=table_name),
                 logging_level='info')
    return db_table
