from boto3.dynamodb.conditions import Key
from moto import mock_dynamodb2

from cc_dynamodb3.mocks import mock_table_with_data


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
def test_query_sorts(fake_config):
    mock_data()
    import cc_dynamodb3
    results = cc_dynamodb3.query_table('change_in_condition',
                                       saved_in_rdb__eq=0,
                                       query_index='SavedInRDB')['Items']

    times = [result.get('time') for result in results]
    assert times == [1, 2, 4]


def _test_comparator_helper(**query_kwargs):
    mock_data()
    import cc_dynamodb3
    results = cc_dynamodb3.query_table('change_in_condition',
                                       query_index='SavedInRDB',
                                       saved_in_rdb=0,
                                       **query_kwargs)['Items']

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
    import cc_dynamodb3
    results = cc_dynamodb3.query_table('change_in_condition',
                                       saved_in_rdb__eq=0,
                                       query_index='SavedInRDB',
                                       descending=True)['Items']

    times = [result.get('time') for result in results]
    assert times == [4, 2, 1]


@mock_dynamodb2
def test_mock_query_count_filters_equal(fake_config):
    mock_data()
    import cc_dynamodb3
    assert cc_dynamodb3.query_table('change_in_condition',
                                    saved_in_rdb__eq=0,
                                    query_index='SavedInRDB')['Count'] == 3
