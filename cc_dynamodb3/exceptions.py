class UnknownTableException(Exception):
    pass


class TableAlreadyExistsException(Exception):
    def __init__(self, response):
        self.response = response


class UpdateTableException(Exception):
    pass


class ConfigurationError(Exception):
    pass
