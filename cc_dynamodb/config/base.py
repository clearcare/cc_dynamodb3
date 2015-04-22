import os.path

CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'dynamodb.yml')

LOGGING = {
        'LOGSTASH': {
            'filename': '/tmp/cc_dynamodb.log',
            'maxBytes': 100000,
            'backupCount': 2,
        },
}