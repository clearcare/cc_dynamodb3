from boto3.session import Session

from .config import get_config


_cached_client = None
_cached_resource = None


def get_connection(as_resource=True, use_cache=True):
    """Returns a DynamoDBConnection even if credentials are invalid."""
    global _cached_client
    global _cached_resource

    if use_cache:
        if as_resource and _cached_resource:
            return _cached_resource

        if not as_resource and _cached_client:
            return _cached_client

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
            _cached_client = session.client('dynamodb',
                                            endpoint_url=endpoint_url)
            return _cached_client
        _cached_resource = session.resource('dynamodb',
                                            endpoint_url=endpoint_url,
                                            verify=False)
        return _cached_resource

    if not as_resource:
        _cached_client = session.client('dynamodb')
        return _cached_client

    _cached_resource = session.resource('dynamodb')
    return _cached_resource
