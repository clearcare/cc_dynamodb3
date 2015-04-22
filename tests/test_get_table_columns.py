from moto import mock_dynamodb
import pytest

import cc_dynamodb

@mock_dynamodb
@pytest.mark.xfail(raises=cc_dynamodb.UnknownTableException)
def test_get_dynamodb_table_unknown_table_raises_exception(fake_config):
    cc_dynamodb.get_table('invalid_table')

@pytest.mark.xfail(raises=cc_dynamodb.UnknownTableException)
def test_get_dynamodb_table_unknown_table_raises_exception(fake_config):
    cc_dynamodb.get_table_columns('invalid_table')

def test_get_dynamodb_table_columns_should_return_columns(fake_config):
    columns = cc_dynamodb.get_table_columns('telephony_call_logs')
    assert set(columns.keys()) == set(['direction', 'contents'])
