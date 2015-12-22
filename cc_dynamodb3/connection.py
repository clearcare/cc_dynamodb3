from boto3.session import Session

from .config import get_config


def get_connection(as_resource=True):
    """Returns a DynamoDBConnection even if credentials are invalid."""
    config = get_config()

    session = Session(
        aws_access_key_id=config.aws_access_key_id,
        aws_secret_access_key=config.aws_secret_access_key,
        region_name='us-west-2',
    )

    if config.host:
        endpoint_url = '%s://%s:%s' % (
            'https' if config.is_secure else 'http',  # Host where DynamoDB Local resides
            config.host,                              # DynamoDB Local port (8000 is the default)
            config.port,                              # For DynamoDB Local, disable secure connections
        )

        if not as_resource:
            return session.client('dynamodb',
                                  endpoint_url=endpoint_url)
        return session.resource('dynamodb',
                                endpoint_url=endpoint_url)

    if not as_resource:
        return session.client('dynamodb')

    return session.resource('dynamodb')
