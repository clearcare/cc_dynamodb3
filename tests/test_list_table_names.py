def test_success(fake_config):
    import cc_dynamodb3
    table_names = cc_dynamodb3.list_table_names()
    assert 'nps_survey' in table_names
