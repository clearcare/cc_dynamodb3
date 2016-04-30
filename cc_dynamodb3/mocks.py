from cc_dynamodb3 import (
    get_connection,
    get_table_name,
    get_table_config,
)


def mock_table_with_data(table_name, data):
    """Create a table and populate it with array of items from data.

    Example:

    data = [{'key_1': 'value 1'}, {'key_1': 'value 2'}]
    table = mock_table_with_data('some_table', data)

    len(table.scan())  # Expect 2 results
    """
    dynamodb = get_connection()
    dynamodb.create_table(**get_table_config(table_name))
    table = dynamodb.Table(get_table_name(table_name))
    for item_data in data:
        table.put_item(Item=item_data)
    return table
