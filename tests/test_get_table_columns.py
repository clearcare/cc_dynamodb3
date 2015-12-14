from moto import mock_dynamodb2
import pytest

import cc_dynamodb3


@mock_dynamodb2
def test_get_dynamodb_table_unknown_table_raises_exception(fake_config):
    with pytest.raises(cc_dynamodb3.UnknownTableException):
        cc_dynamodb3.get_table('invalid_table')


def test_get_dynamodb_columns_unknown_table_raises_exception(fake_config):
    with pytest.raises(cc_dynamodb3.UnknownTableException):
        cc_dynamodb3.get_table_columns('invalid_table')


def test_get_dynamodb_table_columns_should_return_columns(fake_config):
    columns = cc_dynamodb3.get_table_columns('nps_survey')
    assert set(columns.keys()) == {'favorite', 'change', 'comments', 'recommend_score'}
