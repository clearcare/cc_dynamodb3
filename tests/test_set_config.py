import cc_dynamodb3.config


def test_success():
    config = cc_dynamodb3.config.get_config()
    assert config.aws_access_key_id == '<KEY>'
    assert config.aws_secret_access_key == '<SECRET>'
    assert config.namespace == 'dev_'