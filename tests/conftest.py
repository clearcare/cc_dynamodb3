import pytest


def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true", help="run slow tests")


@pytest.fixture
def fake_config():
    import cc_dynamodb
    cc_dynamodb.set_config(
        aws_access_key_id='<KEY>',
        aws_secret_access_key='<SECRET>',
        namespace='dev_')
