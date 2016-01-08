import cc_dynamodb3.config

import mock

from .conftest import AWS_DYNAMODB_CONFIG_PATH


@mock.patch('cc_dynamodb3.config.yaml.load')
@mock.patch('cc_dynamodb3.config.get_redis_cache')
def test_load_with_redis_does_not_call_yaml_load(get_redis_cache, yaml_load):
    redis_mock = mock.Mock()
    redis_mock.get = lambda key: '{"foo": "bar"}'
    get_redis_cache.return_value = redis_mock

    cc_dynamodb3.config.set_config(
        config_file_path='/path/to/file.yaml',
        aws_access_key_id='<KEY>',
        aws_secret_access_key='<SECRET>',
        namespace='dev_')

    config = cc_dynamodb3.config.get_config()

    assert not yaml_load.called
    assert config.aws_access_key_id == '<KEY>'
    assert config.yaml['foo'] == 'bar'


@mock.patch('cc_dynamodb3.config.get_redis_cache')
def test_load_with_redis_calls_yaml_load_if_cache_miss(get_redis_cache):
    redis_mock = mock.Mock()
    redis_mock.get = lambda key: None
    get_redis_cache.return_value = redis_mock

    cc_dynamodb3.config.set_redis_config(dict())

    cc_dynamodb3.config.set_config(
        config_file_path=AWS_DYNAMODB_CONFIG_PATH,
        aws_access_key_id='<KEY>',
        aws_secret_access_key='<SECRET>',
        namespace='dev_')

    config = cc_dynamodb3.config.get_config()

    assert redis_mock.setex.called
    assert config.aws_access_key_id == '<KEY>'
    assert config.yaml['default_throughput'] == {'read': 10, 'write': 10}
