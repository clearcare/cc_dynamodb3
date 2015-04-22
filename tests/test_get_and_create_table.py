from decimal import Decimal
from functools import partial

from boto.dynamodb2.table import Item
from mock import patch
from moto import mock_dynamodb2

import cc_dynamodb


def mock_table_with_data(table_name, data):
    '''Create a table (using prefixed method) and populate it with array of items from data.

    Example:

    data = [{'key_1': 'value 1'}, {'key_1': 'value 2'}]
    table = mock_table_with_data('some_table', data)

    len(table.scan())  # Expect 2 results
    '''
    table = cc_dynamodb.create_table(table_name)

    for item_data in data:
        table.put_item(item_data)

    return table

def query_2_mock(table, *args, **kwargs):
	if kwargs.get('index'):  # indexing is not supported, just return scan()
		return table.scan()
	return table.query_2(*args, **kwargs)


def mock_query_2_index(table_name):
	table = cc_dynamodb.get_table(table_name)
	patcher = patch.object(cc_dynamodb, 'get_table')
	patcher.start()
	mock_table = cc_dynamodb.get_tabletable(table_name)
	mock_table.query_2.side_effect = partial(query_2_mock, table=table)
	return patcher


DYNAMODB_FIXTURES = {
    'nps_survey': [
        {
            'agency_id': Decimal('1669'),
            'change': "I can't think of any...",
            'comments': 'No comment',
            'created': '2014-12-19T22:10:42.705243+00:00',
            'favorite': 'I like all of ClearCare!',
            'profile_id': Decimal('2616346'),
            'recommend_score': '9'
        },
        {
            'agency_id': Decimal('1669'),
            'change': 'Most of the features, please',
            'created': '2014-12-19T22:10:42.705243+00:00',
            'profile_id': Decimal('2616347'),
            'recommend_score': '3'
        },
    ],
}


@mock_dynamodb2
def test_mock_create_table_implements_table_scan(fake_config):
    data = DYNAMODB_FIXTURES['nps_survey']
    data_by_profile_id = {i['profile_id']: i for i in data}
    table = mock_table_with_data('nps_survey', data)

    results = list(table.scan())
    assert len(results) == 2

    for result in results:
        item = data_by_profile_id[result.get('profile_id')]
        assert isinstance(result, Item)
        assert item['agency_id'] == result.get('agency_id')
        assert item['recommend_score'] == result.get('recommend_score')
        assert item.get('favorite') == result.get('favorite')