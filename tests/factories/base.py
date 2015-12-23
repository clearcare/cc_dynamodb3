from cc_dynamodb3.table import create_table

import factory


class BaseFactory(factory.Factory):
    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        inst = model_class.create(**kwargs)
        return inst

    @classmethod
    def create_table(cls):
        return create_table(cls._meta.model.TABLE_NAME)

