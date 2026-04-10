class ExtractionError(Exception):
    """Generic extraction error."""


class ApiRequestError(ExtractionError):
    """Raised when DataMission API request fails."""


class ConfigurationError(ExtractionError):
    """Raised when required configuration is missing or invalid."""


class StorageError(ExtractionError):
    """Raised when MinIO operations fail."""


class ValidationError(ExtractionError):
    """Raised when dataset validation fails."""
