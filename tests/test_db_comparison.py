from __future__ import absolute_import

import datetime

from cc_dynamodb3.models import return_different_fields_except

from .factories.hash_only_model import HashOnlyModelFactory


def test_return_different_fields_except_should_ignore_and_return_true():
    HashOnlyModelFactory.create_table()
    obj1 = HashOnlyModelFactory(agency_subdomain='metzler', external_id=123)
    obj2 = HashOnlyModelFactory(agency_subdomain='metzler', external_id=123)

    assert obj1.created != obj2.created
    assert obj1.updated != obj2.updated
    assert not return_different_fields_except(obj1.item, obj2.item, ['created', 'updated'])


def test_return_different_fields_except_should_return_diff():
    HashOnlyModelFactory.create_table()
    obj1 = HashOnlyModelFactory(agency_subdomain='metzler', external_id=123,
                                created=datetime.datetime.utcnow() - datetime.timedelta(minutes=10))
    obj2 = HashOnlyModelFactory(agency_subdomain='metzler', external_id=123)

    assert obj1.created != obj2.created
    assert return_different_fields_except(obj1.item, obj2.item, ['updated']) == dict(
        old=dict(created=obj2.item['created']),
        new=dict(created=obj1.item['created'])
    )
