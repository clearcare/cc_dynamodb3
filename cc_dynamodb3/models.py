import copy
import datetime
import decimal
import json
import time
import types
import uuid

from schematics.models import Model
from schematics import types as fields

from botocore.exceptions import ClientError

from . import exceptions
from .config import get_config
from .log import log_data
from .table import get_table, query_table


class DynamoDBModel(Model):
    TABLE_NAME = None  # This is required for subclasses.
    FIELDS_SAFE_TO_OVERWRITE = []

    @classmethod
    def from_row(cls, row, metadata=None):
        """Take a row from the DB and return an instaniated object with all of
        the attributes populated from the row's data.

        :param row: A dictionary representing dynamodb data
        :param metadata: (optional) A dictionary representing metadata associated with the DynamoDB request
        :returns: An instantiated subclass of ``DynamoDBModel``

        """
        return cls(row, metadata=metadata)

    @classmethod
    def _value_to_dynamodb(cls, key, value):
        """
        Convert python/schematics values to DynamoDB values.

        :param key: field name (string)
        :param value: field value
        :return: dynamodb-friendly value
        """
        if isinstance(getattr(cls, key), (fields.DateTimeType,
                                          fields.DateType)):
            if not value:
                return 0

            value = getattr(cls, key).to_native(value)
            return decimal.Decimal(int(time.mktime(value.timetuple())))

        if isinstance(getattr(cls, key), fields.UUIDType) and value:
            return getattr(cls, key).to_primitive(value)

        if isinstance(getattr(cls, key), fields.BooleanType):
            return decimal.Decimal('1') if value else decimal.Decimal('0')

        if value == '':  # Empty AttributeValue is an error in DynamoDB
            return None
        return value

    @classmethod
    def _key_value_to_dynamodb(cls, obj, key, value):
        # DynamoDB doesn't treat None as e.g. strings or numbers, so we don't
        # set those values.
        if getattr(cls, key, None) is not None:
            obj[key] = cls._value_to_dynamodb(key, value)

    @classmethod
    def table(cls):
        if not getattr(cls, 'TABLE_NAME', None):
            raise exceptions.MissingTableNameException('Missing table name on %s' % cls)
        return cls.get_table(cls.TABLE_NAME)

    @classmethod
    def get_table(cls, table_name):
        if not table_name:
            raise ValueError('Missing TABLE_NAME on class: %s' % cls)
        return get_table(cls.TABLE_NAME)

    @classmethod
    def get(cls, **kwargs):
        """
        Retrieve a DynamoDB item via GetItem.

        :param kwargs: primary key fields.
        :return:
        """
        table_keys = [key['name'] for key in cls.get_schema()]

        if kwargs.keys() != table_keys:
            raise exceptions.ValidationError('Invalid get kwargs: %s, expecting: %s' %
                                             (', '.join(kwargs.keys()), ', '.join(table_keys)))

        response = cls.table().get_item(Key=kwargs)
        if not response or 'Item' not in response:
            raise exceptions.NotFound('Item not found with kwargs: %s' % kwargs)

        row = response['Item']
        metadata = response.get('ResponseMetadata', {})
        return cls.from_row(row, metadata)

    @classmethod
    def _initial_data_to_dynamodb(cls, data):
        dynamodb_data = dict()
        for key, value in data.items():
            cls._key_value_to_dynamodb(dynamodb_data, key, value)
        return dynamodb_data

    @classmethod
    def all(cls):
        response = cls.table().scan()
        metadata = response.get('ResponseMetadata', {})
        for row in response['Items']:
            yield cls.from_row(row, metadata)

    @classmethod
    def query(cls, query_index=None, descending=False, limit=None, **query_keys):
        query_index = query_index or getattr(cls, 'QUERY_INDEX', None)
        response = query_table(cls.TABLE_NAME,
                               query_index=query_index,
                               descending=descending,
                               limit=limit,
                               **query_keys)
        metadata = response.get('ResponseMetadata', {})
        for row in response['Items']:
            yield cls.from_row(row, metadata)

    @classmethod
    def query_count(cls, query_index=None, descending=False, limit=None, **query_keys):
        query_index = query_index or getattr(cls, 'QUERY_INDEX', None)
        response = query_table(cls.TABLE_NAME,
                               query_index=query_index,
                               descending=descending,
                               limit=limit,
                               **query_keys)
        return response['Count']

    @classmethod
    def create(cls, **kwargs):
        dynamodb_data = cls._initial_data_to_dynamodb(kwargs)
        model = cls(dynamodb_data)
        model.save(overwrite=True)
        return model

    @classmethod
    def create_blank(cls):
        """Returns a blank new object"""
        return cls(dict())

    @classmethod
    def gen_uuid(cls):
        return uuid.uuid4()

    @staticmethod
    def utcnow():
        """Easy way to mock utcnow. Useful for testing."""
        return datetime.datetime.utcnow()

    def __init__(self, row, metadata=None):
        for field_name in self._fields.keys():
            if isinstance(self._fields[field_name],
                          fields.UUIDType) and row.get(field_name) is None:
                if field_name.startswith('id'):
                    row[field_name] = self.gen_uuid()
        super(DynamoDBModel, self).__init__(self._dynamodb_to_model(row),
                                            strict=False)
        self.item = row
        self.metadata = metadata
        self._is_deleted = False

        self._set_model_defaults(defaults=self._data)
        self._last_saved_item = copy.deepcopy(self.item)
        self._expect_exists_in_db = self.metadata is not None

    def _set_model_defaults(self, defaults):
        for key, value in defaults.items():
            if value:
                self._key_value_to_dynamodb(self.item, key, value)

    def _dynamodb_to_model(self, row):
        dict_row = dict(row)
        for field_name, dynamodb_value in row.items():
            if field_name in self._fields:
                if isinstance(self._fields[field_name],
                              (fields.DateTimeType,
                               fields.DateType)):
                    if dynamodb_value:
                        dict_row[field_name] = datetime.datetime.fromtimestamp(
                            float(dynamodb_value)
                        )  # TODO: test for this
                    else:
                        dict_row[field_name] = None
                if isinstance(self._fields[field_name],
                              fields.BooleanType):
                    # tests/test_gis_report_provider.py covers this
                    dict_row[field_name] = bool(
                        dynamodb_value
                    )  # DynamoDB loads boolean as e.g. Decimal('1')
        return dict_row

    def __setattr__(self, key, value):
        super(DynamoDBModel, self).__setattr__(key, value)
        if hasattr(self, 'item'):
            self._key_value_to_dynamodb(self.item, key, value)

    def validate(self, partial=False, strict=False, overwrite=False):
        if self._is_deleted and not overwrite:
            raise exceptions.ValidationError('%s already deleted. Pass overwrite=True to force.' % self.__class__.__name__)

        try:
            super(DynamoDBModel, self).validate()
        except exceptions.ModelValidationError as e:
            raise exceptions.ValidationError(e.messages)

    def to_json(self, role=None, context=None):
        serialized = self.serialize(role=role, context=context)
        return to_json(serialized)

    @classmethod
    def get_schema(cls):
        config_yaml = get_config().yaml
        return config_yaml['schemas'][cls.TABLE_NAME]

    def get_primary_key(self):
        """Return a dictionary used for cls.get by an item's primary key."""
        return dict(
            (key['name'], self._value_to_dynamodb(key['name'], getattr(self, key['name'])))
            for key in self.get_schema()
        )

    def reload(self):
        try:
            return self.get(**self.get_primary_key())
        except exceptions.NotFound:
            return None

    def delete(self):
        if self._is_deleted:
            return False
        table = self.table()
        schema = table.key_schema
        table.delete_item(Key=dict(
            (key['AttributeName'], self.item[key['AttributeName']])
            for key in schema
        ))
        self._is_deleted = True
        return True

    def get_unsaved_fields(self):
        different_fields = return_different_fields_except(self.item, self._last_saved_item)
        return different_fields.get('new') or dict()

    def get_attribute_updates(self):
        return dict(
            (field_name, dict(Value=field_value, Action='PUT'))
            for field_name, field_value in self.get_unsaved_fields().items()
        )

    def has_changed_primary_key(self):
        """Returns True if the primary key of this object has been changed (check before saving)."""
        primary_key = self.get_primary_key()
        attribute_updates = self.get_attribute_updates()
        if set(attribute_updates.keys()) & set(primary_key.keys()):
            return True
        return False

    def update(self, skip_primary_key_check=False):
        """
        Update an existing item via boto. Called by save(), mostly for internal use.

        WARNING: Will not work if the item doesn't exist.
        :param skip_primary_key_check:
        :return:
        """
        attribute_updates = self.get_attribute_updates()
        if not attribute_updates:
            return dict()

        if not skip_primary_key_check and self.has_changed_primary_key():
            raise exceptions.PrimaryKeyUpdateException(
                    'Cannot change primary key, use %s.save(overwrite=True)' % self.TABLE_NAME)

        response = self.table().update_item(
            Key=self.get_primary_key(),
            AttributeUpdates=attribute_updates,
            ReturnValues='ALL_OLD',
        )
        self._last_saved_item = copy.deepcopy(self.item)
        self._expect_exists_in_db = True
        return response

    def save(self, overwrite=False):
        """
        Save this object to the database.

        :param overwrite: set to True to force re-save deleted objects.
        """
        self.validate(overwrite=overwrite)

        has_changed_primary_key = self.has_changed_primary_key()
        if has_changed_primary_key:
            log_data('Primary key changed for table=%s, overwrite=%s' %
                     (self.table().name, overwrite),
                     extra=dict(
                         new=dict(self.item.items()),
                         old=dict(self._last_saved_item.items()),
                     ),
                     logging_level='warning')

        try:
            if overwrite or has_changed_primary_key or not self._expect_exists_in_db:
                result = self.table().put_item(Item=self.item,
                                               ReturnValues='ALL_OLD')
            else:
                result = self.update(skip_primary_key_check=has_changed_primary_key)

        except ClientError as e:
            if getattr(e, 'response', None) and e.response.get('Error', {}).get('Code') == 'ValidationException':
                message = repr(e)
                raise exceptions.ValidationError(message)
            raise
        except Exception:
            existing = self.reload()
            different_fields = existing and return_different_fields_except(self.item, existing.item,
                                                                           self.FIELDS_SAFE_TO_OVERWRITE)
            log_data('Error saving, table=%s, overwrite=%s' %
                     (self.table().name, overwrite),
                     extra=dict(
                         save_new=dict(self.item.items()),
                         save_old=dict(existing.item.items()) if existing else None,
                         different_fields=different_fields,
                     ),
                     logging_level='error')

            raise

        if overwrite:
            log_data('save overwrite=True table=%s' % self.table().name,
                     extra=dict(
                         db_item=dict(self.item.items()),
                         put_item_result=result,
                     ),
                     logging_level='warning')

        if result.get('ResponseMetadata', {}):
            self.metadata = result['ResponseMetadata']

        if not overwrite and 'Attributes' in result and result['Attributes'] != self.item:
            different_fields = return_different_fields_except(self.item, result['Attributes'],
                                                              self.FIELDS_SAFE_TO_OVERWRITE)
            if different_fields and different_fields.get('old'):
                log_data('Save overwrote data, table=%s, overwrite=%s' %
                         (self.table().name, overwrite),
                         extra=dict(
                             saved_new=dict(self.item.items()),
                             saved_old=dict(result['Attributes'].items()),
                             old_fields=different_fields,
                             new_fields=self.get_unsaved_fields(),
                         ),
                         logging_level='error')
        # Save succeeded, update locally
        self._is_deleted = False
        self._last_saved_item = copy.deepcopy(self.item)
        self._expect_exists_in_db = True
        return result


class DynamoDBJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DynamoDBJSONEncoder, self).default(o)


def to_json(serialized):
    if isinstance(serialized, types.GeneratorType):
        serialized = list(serialized)
    return json.dumps(serialized, cls=DynamoDBJSONEncoder)


def return_different_fields_except(new, old, fields_to_ignore=None):
    new_dict = dict(new.iteritems())
    old_dict = dict(old.iteritems())
    fields_to_ignore = fields_to_ignore or []
    for field in fields_to_ignore:
        if field in new_dict:
            del new_dict[field]
        if field in old_dict:
            del old_dict[field]
    fields_to_delete = []
    for field, value in old_dict.iteritems():
        if value == new_dict.get(field):
            fields_to_delete.append(field)
    for field in fields_to_delete:
        del new_dict[field]
        del old_dict[field]

    if new_dict or old_dict:
        return dict(
            new=new_dict,
            old=old_dict,
        )
    return dict()