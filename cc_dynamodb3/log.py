import logging


from .config import get_config


def create_logger(namespace=None):
    name = 'cc_dynamodb3'
    if namespace:
        name += '.' + namespace
    return logging.getLogger(name)


logger = create_logger()


def log_data(message, logging_level=logging.DEBUG, exc_info=True, extra=None):
    """
    Central place to log, with callback configuration support.

    :param message: logging message
    :param logging_level: logging level
    :param exc_info: include exception info
    :param extra: extra data, useful for e.g. sentry
    """
    if isinstance(logging_level, basestring):
        try:
            logging_level = getattr(logging, logging_level.upper())
        except AttributeError:
            logging_level = logging.ERROR
    config = get_config()

    extra = extra or dict()
    extra.setdefault('namespace', config.namespace)
    if config.log_extra_callback:
        extra.update(config.log_extra_callback())

    logger.log(logging_level, message, exc_info=exc_info, extra=extra)

