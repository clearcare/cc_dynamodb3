from .conftest import DYNAMODB_FIXTURES
from cc_dynamodb3.mocks import mock_table_with_data
from cc_dynamodb3.table import scan_all_in_table, query_all_in_table


def test_scan_all_works_on_case_with_little_data():
    data = DYNAMODB_FIXTURES['nps_survey']
    data_by_profile_id = {i['profile_id']: i for i in data}
    table = mock_table_with_data('nps_survey', data)

    results = list(scan_all_in_table(table))
    assert len(results) == 2

    for result, metadata in results:
        item = data_by_profile_id[result.get('profile_id')]
        assert item['agency_id'] == result.get('agency_id')
        assert item['recommend_score'] == result.get('recommend_score')
        assert item.get('favorite') == result.get('favorite')


def test_query_all_works_on_case_with_little_data():
    data = DYNAMODB_FIXTURES['nps_survey']
    data_by_profile_id = {i['profile_id']: i for i in data}
    table = mock_table_with_data('nps_survey', data)

    results = list(query_all_in_table(table, agency_id=1669))
    assert len(results) == 2

    for result, metadata in results:
        item = data_by_profile_id[result.get('profile_id')]
        assert item['agency_id'] == result.get('agency_id')
        assert item['recommend_score'] == result.get('recommend_score')
        assert item.get('favorite') == result.get('favorite')

    results = list(query_all_in_table(table, agency_id=1000))
    assert len(results) == 0


def test_scan_all_paginate():
    data = DYNAMODB_FIXTURES['nps_survey']
    data_by_profile_id = {i['profile_id']: i for i in data}
    table = mock_table_with_data('nps_survey', data)

    results = list(scan_all_in_table(table, limit=1, paginate=True))
    assert len(results) == 1

    for result, metadata, last_evaluated_key in results:
        item = data_by_profile_id[result.get('profile_id')]
        assert item['agency_id'] == result.get('agency_id')
        assert item['recommend_score'] == result.get('recommend_score')
        assert item.get('favorite') == result.get('favorite')
        assert last_evaluated_key

    results2 = list(scan_all_in_table(table, limit=1, paginate=True,
                                      exclusive_start_key=last_evaluated_key))
    assert results2[0][0]['profile_id'] != results[0][0]['profile_id']
