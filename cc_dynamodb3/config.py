import copy
import os

from bunch import Bunch
import yaml

from .exceptions import ConfigurationError
from .log import logger


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
