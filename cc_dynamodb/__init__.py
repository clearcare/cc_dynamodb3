"""
Usage:

dynamodb.set_config(
    namespace='dev_',
    aws_access_key_id='<YOUR AWS KEY>'
    aws_secret_access_key='<YOUR AWS SECRET>')

dynamodb.get_table('some_table_name')
"""
import copy
import os

from boto import dynamodb2
from boto.dynamodb2 import fields  # AllIndex, GlobalAllIndex, HashKey, RangeKey
from boto.dynamodb2 import table
from boto.dynamodb2 import types
from bunch import Bunch
import yaml

from .config import Config
from .logging import create_logger


config = Config()
logger = create_logger(name='cc_dynamodb', **config['LOGGING'])

# Cache to avoid parsing YAML file repeatedly.
_cached_config = None

def set_config(**kwargs):
    global _cached_config

    with open(config['CONFIG_PATH']) as config_file:
        yaml_config = yaml.load(config_file)

    _cached_config = Bunch({
        'yaml': yaml_config,
        'namespace': kwargs.get('namespace')
                        or os.environ.get('CC_DYNAMODB_NAMESPACE'),
        'aws_access_key_id': kwargs.get('aws_access_key_id')
                                or os.environ.get('CC_DYNAMODB_ACCESS_KEY_ID'),
        'aws_secret_access_key': kwargs.get('aws_secret_access_key')
                                    or os.environ.get('CC_DYNAMODB_SECRET_ACCESS_KEY'),
    })

    if not _cached_config.namespace:
        raise ConfigurationError('Missing namespace kwarg OR environment variable CC_DYNAMODB_NAMESPACE')
    if not _cached_config.aws_access_key_id:
        raise ConfigurationError('Missing aws_access_key_id kwarg OR environment variable CC_DYNAMODB_ACCESS_KEY_ID')
    if not _cached_config.aws_secret_access_key:
        raise ConfigurationError('Missing aws_secret_access_key kwarg OR environment variable CC_DYNAMODB_SECRET_ACCESS_KEY')

    logger.event('cc_dynamodb.set_config', message='config loaded')


def get_config(**kwargs):
    global _cached_config

    if not _cached_config:
        set_config(**kwargs)

    return Bunch(copy.deepcopy(_cached_config.toDict()))


class ConfigurationError(Exception):
    pass


def _build_key(key_details):
    key_details = key_details.copy()
    key_type = getattr(fields, key_details.pop('type'))
    key_details['data_type'] = getattr(types, key_details['data_type'])
    return key_type(**key_details)


def _build_keys(keys_config):
    return [_build_key(key_details)
            for key_details in keys_config]


def _build_secondary_index(index_details):
    index_type = getattr(fields, index_details.pop('type'))
    parts = []
    for key_details in index_details.get('parts', []):
        parts.append(_build_key(key_details))
    return index_type(index_details['name'], parts=parts)


def _build_secondary_indexes(indexes_config):
    return [_build_secondary_index(index_details)
            for index_details in indexes_config]


def _get_table_metadata(table_name):
    config = get_config().yaml

    try:
        keys_config = config['schemas'][table_name]
    except KeyError:
        logger.exception('cc_dynamodb.UnknownTable', table_name=table_name, config=config.toDict())
        raise UnknownTableException('Unknown table: %s' % table_name)

    schema = _build_keys(keys_config)

    global_indexes_config = config['global_indexes'].get(table_name, [])
    indexes_config = config['indexes'].get(table_name, [])

    return dict(
        schema=schema,
        global_indexes=_build_secondary_indexes(global_indexes_config),
        indexes=_build_secondary_indexes(indexes_config),
    )


def get_table_name(table_name):
    '''Prefixes the table name for the different environments/settings.'''
    return get_config().namespace + table_name


def get_connection():
    """Returns a DynamoDBConnection even if credentials are invalid."""
    config = get_config()
    return dynamodb2.connect_to_region(
        'us-west-2',
        aws_access_key_id=config.aws_access_key_id,
        aws_secret_access_key=config.aws_secret_access_key,
    )


def get_table_columns(table_name):
    config = get_config().yaml
    try:
        return dict(
            (column_name, getattr(types, column_type))
                for column_name, column_type in config['columns'][table_name].items())
    except KeyError:
        logger.exception('cc_dynamodb.UnknownTable', table_name=table_name, config=config.toDict())
        raise UnknownTableException('Unknown table: %s' % table_name)


def get_table(table_name, connection=None):
    '''Returns a dict with table and preloaded schema, plus columns.

    WARNING: Does not check the table actually exists. Querying against
             a non-existent table raises boto.exception.JSONResponseError

    This function avoids additional lookups when using a table.
    The columns included are only the optional columns you may find in some of the items.
    '''
    return table.Table(
        get_table_name(table_name),
        connection=connection or get_connection(),
        **_get_table_metadata(table_name)
    )


def create_table(table_name, connection=None, throughput=False):
    prefixed_table_name = get_table_name(table_name)

    if throughput == False:
    	config = get_config().yaml
        throughput = config.default_throughput.toDict()

    return table.Table.create(
        prefixed_table_name,
        connection=connection or get_connection(),
        throughput=throughput,
        **_get_table_metadata(table_name)
    )


class UnknownTableException(Exception):
    pass