import factory.fuzzy
from schematics import types as fields

from cc_dynamodb3.fields import DynamoDBMapField
from cc_dynamodb3.models import DynamoDBModel

from .base import BaseFactory


class MapFieldModel(DynamoDBModel):
    TABLE_NAME = 'map_field'

    agency_subdomain = fields.StringType(required=True)
    request_data = DynamoDBMapField()


class MapFieldModelFactory(BaseFactory):
    class Meta:
        model = MapFieldModel

    agency_subdomain = factory.fuzzy.FuzzyText(length=8)
