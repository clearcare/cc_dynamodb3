def test_success(fake_config):
    import cc_dynamodb
    table_names = cc_dynamodb.list_table_names()
    assert 'nps_survey' in table_names