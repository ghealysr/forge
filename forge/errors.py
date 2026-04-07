"""Exception hierarchy. All FORGE exceptions inherit from ForgeError."""


class ForgeError(Exception):
    """Base class for all FORGE errors."""


class ConfigError(ForgeError):
    """Configuration is invalid or missing."""


class DatabaseError(ForgeError):
    """Database operation failed."""


class TransactionError(DatabaseError):
    """Transaction commit/rollback failed."""


class ImportError_(ForgeError):
    """CSV or data import failed."""


class EnrichmentError(ForgeError):
    """Enrichment pipeline error."""


class AdapterError(ForgeError):
    """Model adapter (Ollama/Claude) error."""


class DiscoveryError(ForgeError):
    """Business discovery (Overture Maps) error."""


class ExportError(ForgeError):
    """Data export failed."""
