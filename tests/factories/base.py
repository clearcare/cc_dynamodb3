import factory

import cc_dynamodb3


class BaseFactory(factory.Factory):
    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        return model_class.create(**kwargs)

    @classmethod
    def create_table(cls):
        dynamodb = cc_dynamodb3.connection.get_connection()
        return dynamodb.create_table(**cc_dynamodb3.table.get_table_config(cls._meta.model.TABLE_NAME))
