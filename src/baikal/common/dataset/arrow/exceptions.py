class BaseArrowException(Exception):
    pass


class InvalidMetadataException(BaseArrowException):
    pass


class SchemaValidationException(BaseArrowException):
    pass
