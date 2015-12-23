from schematics.exceptions import ModelValidationError


class UnknownTableException(Exception):
    pass


class TableAlreadyExistsException(Exception):
    def __init__(self, response):
        self.response = response


class UpdateTableException(Exception):
    pass


class ConfigurationError(Exception):
    pass


class NotFound(Exception):
    pass


class ValidationError(ModelValidationError):
    pass


class MissingTableNameException(Exception):
    pass


class PrimaryKeyUpdateException(Exception):
    pass
