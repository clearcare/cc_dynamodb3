import cc_logger

_rotating_file_config = {
        'filename': '/tmp/cc_dynamodb.log',
        'maxBytes': 32000,
}

def create_logger(**config):
    logstash_file_config = config.get('LOGSTASH', _rotating_file_config)
    name = config.get('name', 'cc_dynamodb')
    environment = config.get('ENVIRONMENT', 'test')
    level = config.get('LEVEL', None)

    return cc_logger.create_logger(name, logstash_file_config, level=level,
            environment=environment)
