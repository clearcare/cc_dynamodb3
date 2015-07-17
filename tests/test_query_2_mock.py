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


def _test_comparator_helper(**query_kwargs):
    mock_data()
    with mock_query_2():
        import cc_dynamodb
        table = cc_dynamodb.get_table('change_in_condition')
        results = list(table.query_2(saved_in_rdb__eq=0, index='SavedInRDB', **query_kwargs))

    return [result.get('time') for result in results]

@mock_dynamodb2
def test_mock_query_2_filters_equal(fake_config):
    times = _test_comparator_helper(time__eq=2)
    assert times == [2]


@mock_dynamodb2
def test_mock_query_2_filters_greater_than(fake_config):
    times = _test_comparator_helper(time__gt=2)
    assert times == [4]


@mock_dynamodb2
def test_mock_query_2_filters_greater_than_or_equal(fake_config):
    times = _test_comparator_helper(time__gte=2)
    assert times == [2, 4]


@mock_dynamodb2
def test_mock_query_2_filters_less_than(fake_config):
    times = _test_comparator_helper(time__lt=2)
    assert times == [1]


@mock_dynamodb2
def test_mock_query_2_filters_less_than_or_equal(fake_config):
    times = _test_comparator_helper(time__lte=2)
    assert times == [1, 2]


@mock_dynamodb2
def test_mock_query_2_sorts_reverse(fake_config):
    mock_data()
    with mock_query_2():
        import cc_dynamodb
        table = cc_dynamodb.get_table('change_in_condition')
        results = list(table.query_2(saved_in_rdb__eq=0, index='SavedInRDB', reverse=True))

    times = [result.get('time') for result in results]
    assert times == [4, 2, 1]