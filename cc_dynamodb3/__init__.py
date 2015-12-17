import copy
import operator
import os

from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from boto3.session import Session
from bunch import Bunch
import yaml

from .log import create_logger


logger = create_logger()

# Cache to avoid parsing YAML file repeatedly.
_cached_config = None


def set_config(table_config, namespace=None, aws_access_key_id=False, aws_secret_access_key=False,
               host=None, port=None, is_secure=None):
    global _cached_config

    with open(table_config) as config_file:
        yaml_config = yaml.load(config_file)

    _cached_config = Bunch({
        'yaml': yaml_config,
        'namespace': namespace
                        or os.environ.get('CC_DYNAMODB_NAMESPACE'),
        'aws_access_key_id': aws_access_key_id if aws_access_key_id != False
                                else os.environ.get('CC_DYNAMODB_ACCESS_KEY_ID', False),
        'aws_secret_access_key': aws_secret_access_key if aws_secret_access_key != False
                                    else os.environ.get('CC_DYNAMODB_SECRET_ACCESS_KEY', False),
        'host': host or os.environ.get('CC_DYNAMODB_HOST'),
        'port': port or os.environ.get('CC_DYNAMODB_PORT'),
        'is_secure': is_secure or os.environ.get('CC_DYNAMODB_IS_SECURE'),
    })


    if not _cached_config.namespace:
        msg = 'Missing namespace kwarg OR environment variable CC_DYNAMODB_NAMESPACE'
        logger.error('ConfigurationError: ' + msg)
        raise ConfigurationError(msg)
    if _cached_config.aws_access_key_id is False:
        msg = 'Missing aws_access_key_id kwarg OR environment variable CC_DYNAMODB_ACCESS_KEY_ID'
        logger.error('ConfigurationError: ' + msg)
        raise ConfigurationError(msg)
    if _cached_config.aws_secret_access_key is False:
        msg = 'Missing aws_secret_access_key kwarg OR environment variable CC_DYNAMODB_SECRET_ACCESS_KEY'
        logger.error('ConfigurationError: ' + msg)
        raise ConfigurationError(msg)
    if _cached_config.port:
        try:
            _cached_config.port = int(_cached_config.port)
        except ValueError:
            msg = ('Integer value expected for port '
                   'OR environment variable CC_DYNAMODB_PORT. Got %s' % _cached_config.port)
            logger.error('ConfigurationError: ' + msg)
            raise ConfigurationError(msg)

    logger.info('cc_dynamodb.set_config', extra=dict(status='config loaded'))


def get_config(**kwargs):
    global _cached_config

    if not _cached_config:
        set_config(**kwargs)

    return Bunch(copy.deepcopy(_cached_config.toDict()))


class ConfigurationError(Exception):
    pass


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
    if index_type != 'GlobalAllIndex':
        raise NotImplementedError('TODO: support KEYS_ONLY and INCLUDE with Projection')
    return 'ALL'


def _get_table_metadata(table_name):
    config = get_config().yaml

    try:
        keys_config = config['schemas'][table_name]
    except KeyError:
        logger.exception('cc_dynamodb.UnknownTable', extra=dict(table_name=table_name,
                                                                config=config,
                                                                DTM_EVENT='cc_dynamodb.UnknownTable'))
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


def get_connection(as_resource=True):
    """Returns a DynamoDBConnection even if credentials are invalid."""
    config = get_config()

    session = Session(
        aws_access_key_id=config.aws_access_key_id,
        aws_secret_access_key=config.aws_secret_access_key,
        region_name='us-west-2',
    )

    if config.host:
        endpoint_url = '%s://%s:%s' % (
            'https' if config.is_secure else 'http',  # Host where DynamoDB Local resides
            config.host,                              # DynamoDB Local port (8000 is the default)
            config.port,                              # For DynamoDB Local, disable secure connections
        )

        if not as_resource:
            return session.client('dynamodb',
                                  endpoint_url=endpoint_url)
        return session.resource('dynamodb',
                                endpoint_url=endpoint_url)

    if not as_resource:
        return session.client('dynamodb')

    return session.resource('dynamodb')


def get_table_columns(table_name):
    """Return known columns for a table and their data type."""
    config = get_config().yaml
    try:
        return dict(
            (column_name, column_type)
                for column_name, column_type in config['columns'][table_name].items())
    except KeyError:
        logger.exception('UnknownTable: %s' % table_name, extra=dict(config=config,
                                                                     DTM_EVENT='cc_dynamodb.UnknownTable'))
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


def query_table(table_name, query_index=None, descending=False, limit=None, **query_keys):
    """
    Friendly version to query a table using boto3's interface

    :param table_name: (string) unprefixed table name
    :param query_index: (string, optional) optionally specify a GSI (Global) or LSI (Local Secondary Index)
    :param descending: (boolean) sort in descending order (default False
    :param limit: (integer) limit the number of results directly in the query to dynamodb
    :param query_keys: query arguments, syntax: attribute__gte=123 (similar to boto2's interface)
    :return: boto3 query response
    """
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
    if limit is not None:
        query_kwargs['Limit'] = limit
    if query_index:
        query_kwargs['IndexName'] = query_index

    return get_table(table_name).query(**query_kwargs)


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
        logger.info('cc_dynamodb.create_table: %s' % table_name, extra=dict(status='created table'))
        return db_table
    except ClientError as e:
        if (e.response['ResponseMetadata']['HTTPStatusCode'] == 400 and
                e.response['Error']['Code'] == 'ResourceInUseException'):
            logger.warn('Called create_table("%s"), but already exists: %s' %
                        (table_name, e.response))
            raise TableAlreadyExistsException(response=e.response)
        raise e


def _validate_schema(table_name, upstream_schema, local_schema):
    """Raise error if primary index (schema) is not the same as upstream"""
    if sorted(upstream_schema, key=lambda i: i['AttributeName']) != sorted(local_schema, key=lambda i: i['AttributeName']):
        msg = 'Mismatched schema: %s VS %s' % (upstream_schema, local_schema)
        logger.warn(msg)
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
            logger.info('Creating GSI %s for %s' % (index_name, table_name))
            gsi_updates.append({
                'Create': index,
            })
        else:
            upstream_index = upstream_global_indexes_by_name[index_name]
            if index['ProvisionedThroughput'] == upstream_index['ProvisionedThroughput']:
                continue
            gsi_updates.append({
                'Update': {
                    'IndexName': index_name,
                    'ProvisionedThroughput': index['ProvisionedThroughput']
                },
            })
            logger.info('Updating GSI %s throughput for %s to %s' % (index_name, table_name, index['ProvisionedThroughput']))

    for index_name in upstream_global_indexes_by_name.keys():
        if index_name not in local_global_indexes_by_name:
            logger.info('Deleting GSI %s for %s' % (index_name, table_name))
            gsi_updates.append({
                'Delete': {
                    'IndexName': index_name,
                }
            })

    if gsi_updates:
        db_table.update(GlobalSecondaryIndexUpdates=gsi_updates)
        logger.info('cc_dynamodb.update_table: %s' % table_name, extra=dict(status='updated table'))
    return db_table


class UnknownTableException(Exception):
    pass


class TableAlreadyExistsException(Exception):
    def __init__(self, response):
        self.response = response


class UpdateTableException(Exception):
    pass