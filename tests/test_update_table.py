import cc_dynamodb3.config
import cc_dynamodb3.exceptions
import cc_dynamodb3.table

import mock
import pytest


def test_update_table_should_raise_if_table_doesnt_exist():
    with pytest.raises(cc_dynamodb3.exceptions.UnknownTableException):
        cc_dynamodb3.table.update_table('change_in_condition')


def test_update_table_should_not_update_if_same_throughput():
    cc_dynamodb3.table.create_table('change_in_condition')
    cc_dynamodb3.table.update_table('change_in_condition')

    table = cc_dynamodb3.table.get_table('change_in_condition')
    table.load()
    # Ensure the throughput has been updated
    assert table.provisioned_throughput == {'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10,
                                            'NumberOfDecreasesToday': 0}
    assert len(table.global_secondary_indexes) == 1
    assert table.global_secondary_indexes[0]['ProvisionedThroughput'] == {'WriteCapacityUnits': 15,
                                                                          'ReadCapacityUnits': 15}


def test_update_table_should_create_delete_gsi():
    cc_dynamodb3.table.create_table('change_in_condition')

    original_config = cc_dynamodb3.config.get_config()
    patcher = mock.patch('cc_dynamodb3.table.get_config')
    mock_config = patcher.start()
    original_config.yaml['global_indexes']['change_in_condition'] = [{
        'parts': [
            {'type': 'HashKey', 'name': 'rdb_id', 'data_type': 'NUMBER'},
            {'type': 'RangeKey', 'name': 'session_id', 'data_type': 'NUMBER'}],
        'type': 'GlobalAllIndex',
        'name': 'RdbID',
    }]
    mock_config.return_value = original_config

    cc_dynamodb3.table.update_table('change_in_condition', throughput={'read': 55, 'write': 44})
    mock_config.stop()

    table = cc_dynamodb3.table.get_table('change_in_condition')
    table.load()
    # Ensure the throughput has been updated
    assert table.provisioned_throughput == {'ReadCapacityUnits': 55, 'WriteCapacityUnits': 44}
    assert len(table.global_secondary_indexes) == 1
    assert table.global_secondary_indexes[0]['IndexName'] == 'RdbID'
    assert table.global_secondary_indexes[0]['ProvisionedThroughput'] == {'WriteCapacityUnits': 10,
                                                                          'ReadCapacityUnits': 10}


def test_update_table_should_update_gsi():
    cc_dynamodb3.table.create_table('change_in_condition')

    original_config = cc_dynamodb3.config.get_config()
    patcher = mock.patch('cc_dynamodb3.table.get_config')
    mock_config = patcher.start()
    original_config.yaml['global_indexes']['change_in_condition'][0]['throughput'] = {
        'read': 20,
        'write': 20,
    }
    mock_config.return_value = original_config

    cc_dynamodb3.table.update_table('change_in_condition')
    mock_config.stop()

    table = cc_dynamodb3.table.get_table('change_in_condition')
    table.load()
    # Ensure the primary throughput has not been been updated
    assert table.provisioned_throughput == {'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10,
                                            'NumberOfDecreasesToday': 0}
    # ... but the GSI has been
    assert table.global_secondary_indexes[0]['ProvisionedThroughput'] == {'WriteCapacityUnits': 20,
                                                                          'ReadCapacityUnits': 20}
