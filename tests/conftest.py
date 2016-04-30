from decimal import Decimal
import os.path

from moto import mock_dynamodb2
import pytest


AWS_DYNAMODB_TF = os.path.join(os.path.dirname(__file__), 'dynamo_tables.tf')


@pytest.fixture(scope='function', autouse=True)
def fake_config():
    import cc_dynamodb3
    cc_dynamodb3.config.set_config(
        AWS_DYNAMODB_TF,
        aws_access_key_id='<KEY>',
        aws_secret_access_key='<SECRET>',
        namespace='dev_',
    )


@pytest.fixture(scope='function', autouse=True)
def mock_db(request):
    """We never want to use the real dynamodb."""
    mock = mock_dynamodb2()
    mock.start()
    request.addfinalizer(mock.stop)


DYNAMODB_FIXTURES = {
    'nps_survey': [
        {
            'agency_id': Decimal('1669'),
            'change': "I can't think of any...",
            'comments': 'No comment',
            'created': '2014-12-19T22:10:42.705243+00:00',
            'favorite': 'I like all of ClearCare!',
            'profile_id': Decimal('2616346'),
            'recommend_score': '9'
        },
        {
            'agency_id': Decimal('1669'),
            'change': 'Most of the features, please',
            'created': '2014-12-19T22:10:42.705243+00:00',
            'profile_id': Decimal('2616347'),
            'recommend_score': '3'
        },
    ],
}
