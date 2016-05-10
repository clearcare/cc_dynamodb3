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
        """Take a row from the DB and return an instantiated object with all of
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
        if cls._fields.get(key, None) is not None:
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

        if set(kwargs.keys()) != set(table_keys):
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
        # DynamoDB scan only returns up to 1MB of data, so we need to keep scanning.
        while True:
            metadata = response.get('ResponseMetadata', {})
            for row in response['Items']:
                yield cls.from_row(row, metadata)
            if response.get('LastEvaluatedKey'):
                response = cls.table().scan(
                    ExclusiveStartKey=response['LastEvaluatedKey'],
                )
            else:
                break

    @classmethod
    def paginated_query(cls, query_index=None, descending=False, limit=None, exclusive_start_key=None, filter_expression=None, **query_keys):
        """
        Return 'limit' number results along with the Last Evaluated Key.
        Keep your 'limit' reasonable, this returns a list (not a generator, as query() does).
        This method is useful for callers that will be handling making successive calls to get additional results,
        such as in an infinite scrolling page that will make AJAX callbacks to get more results.

        :param query_index: Name of DynamoDB LSI or GSI (pre-namespaced) to use for query key lookup
        :type query_index: String
        :param descending: If True, return results in descending range key order. Default False
        :type descending: Boolean
        :param limit: Count of Items to return
        :type limit: int
        :param exclusive_start_key: LastEvaluatedKey (lek) returned from previous paginated_query()
        :type exclusive_start_key: dict
        :param filter_expression:
        :type filter_expression: dict
        :param query_keys:
        :type query_keys: dict
        :return: list of items fulfilling query, LastEvaluatedKey to use for successive query exclusive_start_key
        :rtype: tuple of list, dict
        """

        query_index = query_index or getattr(cls, 'QUERY_INDEX', None)
        result_list = list()
        # Prime our loop variables. Query isn't fulfilled until remaining_count == 0 or LastEvaluatedKey says no more
        # Because we don't have a LEK yet, initialize it to True. It will be set to something or None from the
        # first query. We special case a passed in limit of zero to ensure that the method doesn't exit with lek
        # set to True.
        remaining_count = limit
        lek = True if limit else None
        while remaining_count and lek:
            response = query_table(cls.TABLE_NAME,
                                   query_index=query_index,
                                   descending=descending,
                                   limit=limit,
                                   exclusive_start_key=exclusive_start_key,
                                   filter_expression=filter_expression,
                                   **query_keys)
            exclusive_start_key = lek = response.get('LastEvaluatedKey')
            returned_count = response['Count']  # This is the count of Items actually returned (post filtering)
            metadata = response.get('ResponseMetadata', {})
            result_list += [cls.from_row(row, metadata) for row in response['Items']]
            remaining_count -= returned_count
        return result_list, lek

    @classmethod
    def query(cls, query_index=None, descending=False, limit=None, filter_expression=None, **query_keys):
        query_index = query_index or getattr(cls, 'QUERY_INDEX', None)
        response = query_table(cls.TABLE_NAME,
                               query_index=query_index,
                               descending=descending,
                               limit=limit,
                               filter_expression=filter_expression,
                               **query_keys)
        total_found = 0
        # DynamoDB scan only returns up to 1MB of data, so we need to keep querying.
        while True:
            metadata = response.get('ResponseMetadata', {})
            for row in response['Items']:
                yield cls.from_row(row, metadata)
                total_found += 1
                if limit and total_found == limit:
                    break
            if limit and total_found == limit:
                break
            if response.get('LastEvaluatedKey'):
                response = query_table(cls.TABLE_NAME,
                                       query_index=query_index,
                                       descending=descending,
                                       limit=limit,
                                       exclusive_start_key=response['LastEvaluatedKey'],
                                       **query_keys)
            else:
                break

    @classmethod
    def query_count(cls, query_index=None, descending=False, limit=None, **query_keys):
        query_index = query_index or getattr(cls, 'QUERY_INDEX', None)
        response = query_table(cls.TABLE_NAME,
                               query_index=query_index,
                               descending=descending,
                               limit=limit,
                               count=True,
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
            (key['name'], self.item[key['name']])
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
        self.table().delete_item(Key=self.get_primary_key())
        self._is_deleted = True
        return True

    def get_unsaved_fields(self):
        different_fields = return_different_fields_except(self.item, self._last_saved_item,
                                                          self.FIELDS_SAFE_TO_OVERWRITE)
        return different_fields.get('new') or dict()

    def log_if_unsafe_save(self, result, is_update):
        different_fields = return_different_fields_except(self.item, result['Attributes'],
                                                          self.FIELDS_SAFE_TO_OVERWRITE)

        saved_new = dict(self.item.items())                 # what we have locally
        saved_old = dict(result['Attributes'].items())      # what was upstream
        new_fields = different_fields.get('new') or dict()  # changed locally vs upstream
        old_fields = different_fields.get('old') or dict()  # changed upstream vs locally
        unsaved_fields = self.get_unsaved_fields()          # changed locally since last save
        if is_update:
            if not new_fields and not old_fields:
                return
            if set(unsaved_fields.keys()).issubset(set(new_fields.keys())):
                return

            log_data('Unsafe UPDATE: potential overwrite of data, table=%s, is_update=%s' %
                     (self.table().name, is_update),
                     extra=dict(
                         saved_new=saved_new,
                         saved_old=saved_old,
                         old_fields=old_fields,
                         new_fields=new_fields,
                         unsaved_fields=unsaved_fields,
                     ),
                     logging_level='error')
            return

        # else: not is_update
        if old_fields or new_fields:  # This is overly verbose logging.
            log_data('Unsafe PUT: potential overwrite of data, table=%s, is_update=%s' %
                     (self.table().name, is_update),
                     extra=dict(
                         saved_new=saved_new,
                         saved_old=saved_old,
                         old_fields=old_fields,
                         new_fields=new_fields,
                         unsaved_fields=unsaved_fields,
                     ),
                     logging_level='error')

    @classmethod
    def _get_dynamodb_field_value(cls, field_name, field_value):
        if field_value is None:
            return dict(Action='DELETE')
        return dict(Value=field_value, Action='PUT')

    def get_attribute_updates(self):
        return dict(
            (field_name, self._get_dynamodb_field_value(field_name, field_value))
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
                is_update = False
            else:
                result = self.update(skip_primary_key_check=has_changed_primary_key)
                is_update = True

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

        if result.get('ResponseMetadata', {}):
            self.metadata = result['ResponseMetadata']

        if overwrite:
            log_data('save overwrite=True table=%s' % self.table().name,
                     extra=dict(
                         db_item=dict(self.item.items()),
                         put_item_result=result,
                     ),
                     logging_level='warning')

        if not overwrite:
            # If there are no differences at all, don't bother logging
            if 'Attributes' in result and result['Attributes'] != self.item:
                self.log_if_unsafe_save(result, is_update)
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
