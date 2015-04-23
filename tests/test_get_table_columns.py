from moto import mock_dynamodb
import pytest

import cc_dynamodb

@mock_dynamodb
def test_get_dynamodb_table_unknown_table_raises_exception(fake_config):
    with pytest.raises(cc_dynamodb.UnknownTableException):
        cc_dynamodb.get_table('invalid_table')

def test_get_dynamodb_table_unknown_table_raises_exception(fake_config):
    with pytest.raises(cc_dynamodb.UnknownTableException):
        cc_dynamodb.get_table_columns('invalid_table')

def test_get_dynamodb_table_columns_should_return_columns(fake_config):
    columns = cc_dynamodb.get_table_columns('telephony_call_logs')
    assert set(columns.keys()) == set(['direction', 'contents'])
