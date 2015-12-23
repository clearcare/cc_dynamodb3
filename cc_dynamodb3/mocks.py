import cc_dynamodb3.table


__all__ = [
    'mock_table_with_data',
]


def mock_table_with_data(table_name, data):
    '''Create a table and populate it with array of items from data.

    Example:

    data = [{'key_1': 'value 1'}, {'key_1': 'value 2'}]
    table = mock_table_with_data('some_table', data)

    len(table.scan())  # Expect 2 results
    '''
    table = cc_dynamodb3.table.create_table(table_name)

    for item_data in data:
        table.put_item(Item=item_data)

    return table
