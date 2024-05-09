import copy
import json
import os

from munch import Munch
import redis
import yaml

from .exceptions import ConfigurationError


CONFIG_CACHE_KEY = 'cc_dynamodb3_yaml_config_cache'

_config_file_path = None
# Cache to avoid parsing YAML file repeatedly.
_cached_config = None

# Redis cache, optional but recommended
# Example: dict(host='localhost', port=6379, db=3)
_redis_config = dict()


def set_redis_config(redis_config):
    global _redis_config
    redis_config.setdefault('cache_seconds', 60)
    _redis_config = redis_config


def get_redis_config():
    global _redis_config
    return _redis_config.copy()


def get_redis_cache():
    redis_config = get_redis_config()
    if not redis_config:
        return None

    del redis_config['cache_seconds']

    try:
        return redis.StrictRedis(**redis_config)
    except Exception:
        return None

_redis_cache = get_redis_cache()


def load_yaml_config():
    global _config_file_path

    redis_cache = get_redis_cache()
    if redis_cache:
        yaml_config = redis_cache.get(CONFIG_CACHE_KEY)
        if yaml_config:
            return json.loads(yaml_config)

    with open(_config_file_path) as config_file:
        yaml_config = yaml.safe_load(config_file)
        if redis_cache:
            redis_config = get_redis_config()
            redis_cache.setex(CONFIG_CACHE_KEY, redis_config['cache_seconds'], json.dumps(yaml_config))

    return yaml_config


def set_config(config_file_path, namespace=None, aws_access_key_id=False, aws_secret_access_key=False,
               host=None, port=None, is_secure=None, log_extra_callback=None):
    """
    Set configuration. This is needed only once, globally, per-thread.

    :param config_file_path: This is the path to the configuration file.
    :param namespace: The global table namespace to be used for all tables
    :param aws_access_key_id: (optional) AWS key. boto can grab it from the instance metadata
    :param aws_secret_access_key: (optional) AWS secret. boto can grab it from the instance metadata
    :param host: Host for DynamoDB (useful when running DynamoDB local)
    :param port: Port for DynamoDB (useful when running DynamoDB local)
    :param is_secure: boolean, useful when running DynamoDB local
    :param log_extra_callback: callback function to grab extra data for a log call
    """
    from .log import logger  # avoid circular import

    global _cached_config
    global _config_file_path
    _config_file_path = config_file_path

    yaml_config = load_yaml_config()

    _cached_config = Munch({
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
        'log_extra_callback': log_extra_callback,
    })

    _validate_config()

    extra = dict(status='config loaded', namespace=_cached_config.namespace)
    if log_extra_callback:
        extra.update(**log_extra_callback())

    logger.info('set_config', extra=extra)


def _validate_config():
    from .log import logger  # avoid circular import

    global _cached_config

    if not _cached_config.namespace:
        msg = 'Missing namespace kwarg OR environment variable CC_DYNAMODB_NAMESPACE'
        logger.error('ConfigurationError: ' + msg)
        raise ConfigurationError(msg)
    if _cached_config.aws_access_key_id is False:
        # TODO: Is this really necessary? In the case of IAM authentication, no access key wanted
        msg = 'Missing aws_access_key_id kwarg OR environment variable CC_DYNAMODB_ACCESS_KEY_ID'
        logger.error('ConfigurationError: ' + msg)
        raise ConfigurationError(msg)
    if _cached_config.aws_secret_access_key is False:
        # TODO: Is this really necessary? In the case of IAM authentication, no secret key wanted
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


def get_config(**kwargs):
    global _cached_config

    if not _cached_config:
        # TODO: get_config() should never set_config()
        # Since it's checking _cached_config, and won't set_config() if _cached_config is set,
        # it really doesn't make sense that this ever get called if the config is already set.
        # And get_config() with zero arguments when config is not set will cause TypeError.
        # Makes far more sense for this to just always *only* get, and require set_config()
        # be invoked before calling get_config().
        set_config(**kwargs)

    return Munch(copy.deepcopy(_cached_config.toDict()))
