from .base import *

IS_TEST = False

LOGGING = {
        'LOGSTASH': {
            'filename': '/var/log/cc_dynamodb/dynamodb.log',
            'maxBytes': 50000000,
            'backupCount': 7,
        },
        'ENVIRONMENT': 'prod',
}