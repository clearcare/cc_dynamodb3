def test_success():
    import cc_dynamodb
    cc_dynamodb.set_config(
        aws_access_key_id='<KEY>',
        aws_secret_access_key='<SECRET>',
        namespace='dev_')

    config = cc_dynamodb.get_config()
    assert config.aws_access_key_id == '<KEY>'
    assert config.aws_secret_access_key == '<SECRET>'
    assert config.namespace == 'dev_'
