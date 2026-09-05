class ProviderError(RuntimeError):
    """Base error for one provider request or normalization operation."""


class TransientProviderError(ProviderError): pass
class PermanentProviderError(ProviderError): pass
class RateLimitError(TransientProviderError): pass
class SchemaError(PermanentProviderError): pass
class MappingError(PermanentProviderError): pass
class ValidationError(PermanentProviderError): pass
class EmptyDatasetError(PermanentProviderError): pass
