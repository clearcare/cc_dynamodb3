import importlib
import os

from bunch import Bunch

DEFAULT_ENV = 'cc_dynamodb.config.base'


class Config(Bunch):
    def __init__(self):
        """Just the uppercase variables are stored in the config."""
        variable_name = 'CC_DYNAMODB_CONFIG'

        module_name = os.environ.get(variable_name)
        if not module_name:
            import logging
            logging.warning('The environment variable %r is not set.'
                            'Defaulting to "%s"' %
                            (variable_name, DEFAULT_ENV))
            module_name = DEFAULT_ENV

        try:
            module = importlib.import_module(module_name)
        except ImportError as e:
            e.message = 'Unable to load configuration file (%s)' % e.message
            raise

        for key in dir(module):
            if key.isupper():
                self[key] = getattr(module, key)
