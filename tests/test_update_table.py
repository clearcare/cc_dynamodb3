import cc_dynamodb3

import mock
from moto import mock_dynamodb2
import pytest


@mock_dynamodb2
def test_update_table_should_raise_if_table_doesnt_exist(fake_config):
    with pytest.raises(cc_dynamodb3.UnknownTableException):
        cc_dynamodb3.update_table('change_in_condition')


@mock_dynamodb2
def test_update_table_should_not_update_if_same_throughput(fake_config):
    cc_dynamodb3.create_table('change_in_condition')
    cc_dynamodb3.update_table('change_in_condition')

    table = cc_dynamodb3.get_table('change_in_condition')
    table.load()
    # Ensure the throughput has been updated
    assert table.provisioned_throughput == {'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10,
                                            'NumberOfDecreasesToday': 0}
    assert len(table.global_secondary_indexes) == 1
    assert table.global_secondary_indexes[0]['ProvisionedThroughput'] == {'WriteCapacityUnits': 15,
                                                                          'ReadCapacityUnits': 15}


@mock_dynamodb2
def test_update_table_should_create_delete_gsi(fake_config):
    cc_dynamodb3.create_table('change_in_condition')

    original_config = cc_dynamodb3.get_config()
    patcher = mock.patch('cc_dynamodb3.get_config')
    mock_config = patcher.start()
    original_config.yaml['global_indexes']['change_in_condition'] = [{
        'parts': [
            {'type': 'HashKey', 'name': 'rdb_id', 'data_type': 'NUMBER'},
            {'type': 'RangeKey', 'name': 'session_id', 'data_type': 'NUMBER'}],
        'type': 'GlobalAllIndex',
        'name': 'RdbID',
    }]
    mock_config.return_value = original_config

    cc_dynamodb3.update_table('change_in_condition', throughput={'read': 55, 'write': 44})
    mock_config.stop()

    table = cc_dynamodb3.get_table('change_in_condition')
    table.load()
    # Ensure the throughput has been updated
    assert table.provisioned_throughput == {'ReadCapacityUnits': 55, 'WriteCapacityUnits': 44}
    assert len(table.global_secondary_indexes) == 1
    assert table.global_secondary_indexes[0]['IndexName'] == 'RdbID'
    assert table.global_secondary_indexes[0]['ProvisionedThroughput'] == {'WriteCapacityUnits': 10,
                                                                          'ReadCapacityUnits': 10}


@mock_dynamodb2
def test_update_table_should_update_gsi(fake_config):
    cc_dynamodb3.create_table('change_in_condition')

    original_config = cc_dynamodb3.get_config()
    patcher = mock.patch('cc_dynamodb3.get_config')
    mock_config = patcher.start()
    original_config.yaml['global_indexes']['change_in_condition'][0]['throughput'] = {
        'read': 20,
        'write': 20,
    }
    mock_config.return_value = original_config

    cc_dynamodb3.update_table('change_in_condition')
    mock_config.stop()

    table = cc_dynamodb3.get_table('change_in_condition')
    table.load()
    # Ensure the primary throughput has not been been updated
    assert table.provisioned_throughput == {'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10,
                                            'NumberOfDecreasesToday': 0}
    # ... but the GSI has been
    assert table.global_secondary_indexes[0]['ProvisionedThroughput'] == {'WriteCapacityUnits': 20,
                                                                          'ReadCapacityUnits': 20}
