import cc_dynamodb3.table


def test_success():
    table_names = cc_dynamodb3.table.list_table_names()
    assert 'nps_survey' in table_names
