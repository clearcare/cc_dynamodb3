from functools import partial
from mock import patch

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