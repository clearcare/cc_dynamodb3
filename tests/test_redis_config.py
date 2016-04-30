import cc_dynamodb3.config

import mock

from .conftest import AWS_DYNAMODB_TF


@mock.patch('cc_dynamodb3.config.get_redis_cache')
def test_load_with_redis_does_not_call_yaml_load(get_redis_cache):
    redis_mock = mock.Mock()
    redis_mock.get = lambda key: '{"foo": "bar"}'
    get_redis_cache.return_value = redis_mock

    cc_dynamodb3.config.set_config(
        '/path/to/file.tf',
        aws_access_key_id='<KEY>',
        aws_secret_access_key='<SECRET>',
        namespace='dev_')

    config = cc_dynamodb3.config.get_config()

    assert redis_mock.setex.called == 0
    assert config.aws_access_key_id == '<KEY>'


@mock.patch.object(cc_dynamodb3.config, '_redis_config')
@mock.patch('cc_dynamodb3.config.get_redis_cache')
def test_load_with_redis_calls_yaml_load_if_cache_miss(get_redis_cache, _redis_config):
    redis_mock = mock.Mock()
    redis_mock.get = lambda key: None
    get_redis_cache.return_value = redis_mock

    cc_dynamodb3.config.set_redis_config(dict())

    cc_dynamodb3.config.set_config(
        AWS_DYNAMODB_TF,
        aws_access_key_id='<KEY>',
        aws_secret_access_key='<SECRET>',
        namespace='dev_')

    config = cc_dynamodb3.config.get_config()

    assert redis_mock.setex.called == 0
    assert config.aws_access_key_id == '<KEY>'
