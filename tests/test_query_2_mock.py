from decimal import Decimal

from moto import mock_dynamodb2
import pytest

from cc_dynamodb.mocks import mock_table_with_data, mock_query_2


def mock_data():
    data = [
        {
            'carelog_id': 123,
            'time': 1,
            'saved_in_rdb': 0,
        },
        {
            'carelog_id': 125,
            'time': 4,
            'saved_in_rdb': 0,
        },
        {
            'carelog_id': 127,
            'time': 2,
            'saved_in_rdb': 0,
        },
        {
            'carelog_id': 129,
            'time': 3,
            'saved_in_rdb': 1,
        },
    ]
    mock_table_with_data('change_in_condition', data)


@mock_dynamodb2
def test_mock_query_2_sorts(fake_config):
    mock_data()
    with mock_query_2():
        import cc_dynamodb
        table = cc_dynamodb.get_table('change_in_condition')
        results = list(table.query_2(saved_in_rdb__eq=0, index='SavedInRDB'))

    times = [result.get('time') for result in results]
    assert times == [1, 2, 4]


@mock_dynamodb2
def test_mock_query_2_sorts_reverse(fake_config):
    mock_data()
    with mock_query_2():
        import cc_dynamodb
        table = cc_dynamodb.get_table('change_in_condition')
        results = list(table.query_2(saved_in_rdb__eq=0, index='SavedInRDB', reverse=True))

    times = [result.get('time') for result in results]
    assert times == [4, 2, 1]