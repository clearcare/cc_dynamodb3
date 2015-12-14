def test_success(fake_config):
    import cc_dynamodb3

    config = cc_dynamodb3.get_config()
    assert config.aws_access_key_id == '<KEY>'
    assert config.aws_secret_access_key == '<SECRET>'
    assert config.namespace == 'dev_'