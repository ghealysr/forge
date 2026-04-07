"""FORGE error hierarchy.

All FORGE-specific exceptions inherit from ForgeError.
Use these instead of bare Exception for meaningful error handling.

Public API:
    ForgeError -- base class
    ConfigError -- invalid/missing configuration
    DatabaseError -- database operation failed
    TransactionError -- transaction commit/rollback failed
    ImportError_ -- CSV/data import failed (underscore to avoid shadowing builtin)
    EnrichmentError -- enrichment pipeline error
    AdapterError -- model adapter error (Ollama/Claude)
    DiscoveryError -- Overture/business discovery error
    ExportError -- data export failed
"""


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
