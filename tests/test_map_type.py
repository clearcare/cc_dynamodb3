import pytest

from cc_dynamodb3.cc_types.types import (
    ConversionError,
    MapType,
    ValidationError,
    validate_no_empty_string_values,
)

from .factories.map_type_model import MapTypeModelFactory


def test_validate_raises_for_empty_strings_1():
    with pytest.raises(ValidationError):
        validate_no_empty_string_values({'first_name': ''})


def test_validate_raises_for_empty_strings_2():
    with pytest.raises(ValidationError) as exc_info:
        validate_no_empty_string_values({'nested': {'first_name': ''}})

    #assert 'nested => first_name' in exc_info.value.message[0]
    assert 'nested => first_name' in str(exc_info)


def test_map_field_model_raises_validation_error_with_empty_top_level_attr():
    MapTypeModelFactory.create_table()
    model = MapTypeModelFactory(agency_subdomain='metzler')
    model.request_data = {'first_name': ''}
    with pytest.raises(ValidationError):
        model.save()

def test_map_field_model_raises_validation_error_with_nested_data():
    MapTypeModelFactory.create_table()
    model = MapTypeModelFactory(agency_subdomain='metzler')
    model.request_data = {'nested': {'data': {'first_name': ''}}}
    with pytest.raises(ValidationError):
        model.save()


def test_dynamodb_field_to_native_should_parse_valid_json():
    field = MapType()
    assert field.to_native('{"field": "value"}') == {'field': 'value'}


def test_dynamodb_field_to_native_should_rasie_invalid_json():
    field = MapType()
    with pytest.raises(ConversionError):
        assert field.to_native('{"field": None}')
