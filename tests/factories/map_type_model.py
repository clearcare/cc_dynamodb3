import factory.fuzzy
from schematics import types as fields

from cc_dynamodb3.cc_types import MapType
from cc_dynamodb3.models import DynamoDBModel

from .base import BaseFactory


class MapTypeModel(DynamoDBModel):
    TABLE_NAME = 'map_field'

    agency_subdomain = fields.StringType(required=True)
    request_data = MapType()


class MapTypeModelFactory(BaseFactory):
    class Meta:
        model = MapTypeModel

    agency_subdomain = factory.fuzzy.FuzzyText(length=8)
