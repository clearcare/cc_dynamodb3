from moto import mock_dynamodb2

from conftest import DYNAMODB_FIXTURES
from cc_dynamodb3.mocks import mock_table_with_data


@mock_dynamodb2
def test_mock_create_table_implements_table_scan(fake_config):
    data = DYNAMODB_FIXTURES['nps_survey']
    data_by_profile_id = {i['profile_id']: i for i in data}
    table = mock_table_with_data('nps_survey', data)

    results = list(table.scan()['Items'])
    assert len(results) == 2

    for result in results:
        item = data_by_profile_id[result.get('profile_id')]
        assert item['agency_id'] == result.get('agency_id')
        assert item['recommend_score'] == result.get('recommend_score')
        assert item.get('favorite') == result.get('favorite')