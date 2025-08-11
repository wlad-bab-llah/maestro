"""MAESTRO Custom Exceptions"""

class MaestroError(Exception):
    """Base exception for MAESTRO framework"""
    pass

class ConfigurationError(MaestroError):
    """Raised when configuration is invalid or missing"""
    pass

class FileNotFoundError(MaestroError):
    """Raised when required files are not found"""
    pass

class DAGGenerationError(MaestroError):
    """Raised when DAG generation fails"""
    pass

class TaskCreationError(MaestroError):
    """Raised when task creation fails"""
    pass

class ValidationError(MaestroError):
    """Raised when validation fails"""
    pass
