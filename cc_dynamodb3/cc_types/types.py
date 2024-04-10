import json
import six

from schematics.exceptions import ConversionError, ValidationError
from schematics.types import BaseType
from schematics.types.compound import ListType


class SetType(ListType):

    def _force_set(self, value):
        if value is None or value == set():
            return set()

        try:
            if isinstance(value, six.string_types):
                raise TypeError()

            if isinstance(value, dict):
                return set(value[unicode(k)] for k in sorted(map(int, value.keys())))

            return set(value)
        except TypeError:
            return set(value)

    def to_native(self, value, context=None):
        items = self._force_set(value)

        return set(self.field.to_native(item, context) for item in items)


class MapType(BaseType):
    """A field that stores a valid dict().

    NOTE: this does not convert to a primitive type, it remains a python dictionary.
    However, it can be JSON serialized, as long as it contains JSON serializable data.

    Useful to store data that goes straight into a DynamoDB MAP.
    """
    MESSAGES = {'convert': u"Couldn't interpret '{0}' value as dict().", }

    def _mock(self, context=None):
        return dict()

    def to_native(self, value, context=None):
        if not value:
            return dict()
        if not isinstance(value, dict):
            try:
                value = json.loads(value)
            except (AttributeError, TypeError, ValueError):
                raise ConversionError(self.messages['convert'].format(value))
        return value

    def to_primitive(self, value, context=None):
        # really? primitive is the same?
        return value

    def validate(self, value):
        validate_no_empty_string_values(value)


def validate_no_empty_string_values(value, inside=None):
    """Raise ValidationError if any nested dict value is an empty string."""
    if value == '':
        raise ValidationError('Found empty attribute value%s' %
                              (' for %s' % ' => '.join(inside) if inside else ''))
    if isinstance(value, dict):
        nested_inside = inside or []
        for key, attr in value.items():
            validate_no_empty_string_values(attr, nested_inside + [key])
