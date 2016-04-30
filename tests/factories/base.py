import factory

from cc_dynamodb3 import get_connection, get_table_config


class BaseFactory(factory.Factory):
    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        return model_class.create(**kwargs)

    @classmethod
    def create_table(cls):
        dynamodb = get_connection()
        return dynamodb.create_table(**get_table_config(cls._meta.model.TABLE_NAME))
