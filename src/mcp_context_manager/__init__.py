"""Focused MCP context manager for coding agents."""

from .config import ContextConfig
from .context import ContextService
from .manager import ProjectContextService
from .projects import ProjectRegistry
from .version import SERVER_VERSION

__version__ = SERVER_VERSION

__all__ = [
    "ContextConfig",
    "ContextService",
    "ProjectContextService",
    "ProjectRegistry",
    "SERVER_VERSION",
    "__version__",
]
