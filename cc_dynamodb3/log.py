import logging


def create_logger(namespace=None):
    name = 'cc_dynamodb3'
    if namespace:
        name += '.' + namespace
    return logging.getLogger(name)