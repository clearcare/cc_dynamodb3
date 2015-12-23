import factory.fuzzy
from schematics import types as fields

from cc_dynamodb3.models import DynamoDBModel

from .base import BaseFactory


class HashOnlyModel(DynamoDBModel):
    TABLE_NAME = 'hash_only'

    agency_subdomain = fields.StringType(required=True)
    external_id = fields.IntType()
    name = fields.StringType()
    is_enabled = fields.BooleanType()

    created = fields.DateTimeType(default=DynamoDBModel.utcnow)
    updated = fields.DateTimeType(default=DynamoDBModel.utcnow)


class HashOnlyModelFactory(BaseFactory):
    class Meta:
        model = HashOnlyModel

    agency_subdomain = factory.fuzzy.FuzzyText(length=8)
    external_id = factory.Sequence(lambda n: n + 1)
