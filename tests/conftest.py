import os.path

import pytest


AWS_DYNAMODB_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'dynamodb.yml')


@pytest.fixture
def fake_config():
    import cc_dynamodb
    cc_dynamodb.set_config(
        table_config=AWS_DYNAMODB_CONFIG_PATH,
        aws_access_key_id='<KEY>',
        aws_secret_access_key='<SECRET>',
        namespace='dev_')
