import pytest

import cc_dynamodb3.exceptions
import cc_dynamodb3.table


def test_get_dynamodb_table_unknown_table_raises_exception():
    with pytest.raises(cc_dynamodb3.exceptions.UnknownTableException):
        cc_dynamodb3.table.get_table('invalid_table')


def test_get_dynamodb_columns_unknown_table_raises_exception():
    with pytest.raises(cc_dynamodb3.exceptions.UnknownTableException):
        cc_dynamodb3.table.get_table_columns('invalid_table')


def test_get_dynamodb_table_columns_should_return_columns():
    columns = cc_dynamodb3.table.get_table_columns('nps_survey')
    assert set(columns.keys()) == {'favorite', 'change', 'comments', 'recommend_score'}
