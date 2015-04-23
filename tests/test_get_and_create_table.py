from decimal import Decimal

from boto.dynamodb2.table import Item
from moto import mock_dynamodb2

from cc_dynamodb.mocks import mock_table_with_data


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