import cc_dynamodb

import mock
from moto import mock_dynamodb2
import pytest


@mock_dynamodb2
def test_update_table_should_raise_if_table_doesnt_exist(fake_config):
    with pytest.raises(cc_dynamodb.UnknownTableException):
        cc_dynamodb.update_table('change_in_condition')

@mock_dynamodb2
def test_update_table_should_create_update_delete_gsi(fake_config):
    # NOTE: this test does too many things. Could be broken up.
    # ... but it's nice to cover a case that calls out to all index changes.
    table = cc_dynamodb.create_table('change_in_condition')

    original_metadata = table.describe()
    # Moto does not support GlobalSecondaryIndexes
    original_metadata['Table'].update({
        'GlobalSecondaryIndexes': [
            {'IndexSizeBytes': 111,
             'IndexName': 'SavedInRDB',
             'Projection': {'ProjectionType': 'ALL'},
             'ProvisionedThroughput': {
                 'WriteCapacityUnits': 5,
                 'ReadCapacityUnits': 5,
             },
             'IndexStatus': 'ACTIVE',
             'KeySchema': [
                 {'KeyType': 'HASH', 'AttributeName': 'saved_in_rdb'},
                 {'KeyType': 'RANGE', 'AttributeName': 'time'}],
            'ItemCount': 0},
            {'IndexSizeBytes': 50,
             'IndexName': 'SomeUpstreamIndex',
             'Projection': {'ProjectionType': 'ALL'},
             'ProvisionedThroughput': {
                 'WriteCapacityUnits': 10,
                 'ReadCapacityUnits': 10,
             },
             'IndexStatus': 'ACTIVE',
             'KeySchema': [
                 {'KeyType': 'HASH', 'AttributeName': 'session_id'},
                 {'KeyType': 'RANGE', 'AttributeName': 'time'}],
            'ItemCount': 0}]
    })
    original_config = cc_dynamodb.get_config()
    patcher = mock.patch('cc_dynamodb.get_config')
    mock_config = patcher.start()
    original_config.yaml['global_indexes']['change_in_condition'].append({
        'parts': [
            {'type': 'HashKey', 'name': 'rdb_id', 'data_type': 'NUMBER'},
            {'type': 'RangeKey', 'name': 'session_id', 'data_type': 'NUMBER'}],
        'type': 'GlobalAllIndex',
        'name': 'RdbID',
    })
    mock_config.return_value = original_config

    patcher = mock.patch('cc_dynamodb.table.Table.describe')
    mock_metadata = patcher.start()

    patcher = mock.patch('cc_dynamodb.table.Table.update_global_secondary_index')
    mock_update_gsi = patcher.start()

    patcher = mock.patch('cc_dynamodb.table.Table.create_global_secondary_index')
    mock_create_gsi = patcher.start()

    patcher = mock.patch('cc_dynamodb.table.Table.delete_global_secondary_index')
    mock_delete_gsi = patcher.start()

    mock_metadata.return_value = original_metadata
    cc_dynamodb.update_table('change_in_condition', throughput={'read': 55, 'write': 44})

    mock_metadata.stop()
    mock_update_gsi.stop()
    mock_create_gsi.stop()
    mock_delete_gsi.stop()
    mock_config.stop()

    mock_update_gsi.assert_called_with(global_indexes={'SavedInRDB': {'read': 5, 'write': 5}})
    assert mock_create_gsi.called
    assert mock_create_gsi.call_args[0][0].name == 'RdbID'
    mock_delete_gsi.assert_called_with('SomeUpstreamIndex')

    table = cc_dynamodb.get_table('change_in_condition')
    # Ensure the throughput has been updated
    assert table.throughput == {'read': 5, 'write': 5}