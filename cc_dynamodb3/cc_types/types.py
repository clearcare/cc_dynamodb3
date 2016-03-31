from schematics.types.compound import ListType


class SetType(ListType):

    def _force_set(self, value):
        if value is None or value == set():
            return set()

        try:
            if isinstance(value, basestring):
                raise TypeError()

            if isinstance(value, dict):
                return set(value[unicode(k)] for k in sorted(map(int, value.keys())))

            return set(value)
        except TypeError:
            return set(value)

    def to_native(self, value, context=None):
        items = self._force_set(value)

        return set(self.field.to_native(item, context) for item in items)
