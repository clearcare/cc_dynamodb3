from cc_dynamodb3.table import (
    query_table,
)
from cc_dynamodb3.mocks import mock_table_with_data

# The query tests use the 'time' attribute to uniquely identify test items (rows)
# If it's necessary to add new data, ensure that the time attribute remains unique.


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


def test_query_sorts():
    mock_data()
    results = query_table('change_in_condition',
                          saved_in_rdb__eq=0,
                          query_index='SavedInRDB')['Items']

    times = [result.get('time') for result in results]
    assert times == [1, 2, 4]


def _test_comparator_helper(**query_kwargs):
    mock_data()
    results = query_table('change_in_condition',
                          query_index='SavedInRDB',
                          saved_in_rdb=0,
                          **query_kwargs)['Items']

    return [result.get('time') for result in results]


def test_mock_query_2_equal():
    times = _test_comparator_helper(time__eq=2)
    assert times == [2]


def test_mock_query_2_greater_than():
    times = _test_comparator_helper(time__gt=2)
    assert times == [4]


def test_mock_query_2_greater_than_or_equal():
    times = _test_comparator_helper(time__gte=2)
    assert times == [2, 4]


def test_mock_query_2_less_than():
    times = _test_comparator_helper(time__lt=2)
    assert times == [1]


def test_mock_query_2_less_than_or_equal():
    times = _test_comparator_helper(time__lte=2)
    assert times == [1, 2]


def test_mock_query_2_sorts_reverse():
    mock_data()
    results = query_table('change_in_condition',
                          saved_in_rdb__eq=0,
                          query_index='SavedInRDB',
                          descending=True)['Items']

    times = [result.get('time') for result in results]
    assert times == [4, 2, 1]


def test_mock_query_count_equal():
    mock_data()
    assert query_table('change_in_condition',
                       saved_in_rdb__eq=0,
                       query_index='SavedInRDB')['Count'] == 3
