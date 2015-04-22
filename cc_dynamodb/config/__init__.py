import importlib
import os

from bunch import Bunch


class Config(Bunch):
    def __init__(self):
        """Just the uppercase variables are stored in the config."""
        variable_name = 'CC_DYNAMODB_CONFIG'

        module_name = os.environ.get(variable_name)
        if not module_name:
            raise RuntimeError('The environment variable %r is not set '
                               'and as such configuration could not be '
                               'loaded.  Set this variable and make it '
                               'point to a configuration file' %
                               variable_name)
        try:
            module = importlib.import_module(module_name)
        except ImportError as e:
            e.strerror = 'Unable to load configuration file (%s)' % e.strerror
            raise

        for key in dir(module):
            if key.isupper():
                self[key] = getattr(module, key)
