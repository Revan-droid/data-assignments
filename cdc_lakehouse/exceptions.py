"""Domain-specific exceptions for pipeline control flow."""


class CDCError(Exception):
    """Base exception for CDC workflow failures."""


class SchemaDriftError(CDCError):
    """Raised when the source schema no longer matches the recorded contract."""


class ValidationError(CDCError):
    """Raised when source or warehouse validations fail."""


class ReplayError(CDCError):
    """Raised when replay or checkpoint semantics would be unsafe."""

